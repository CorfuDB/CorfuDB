package org.corfudb.infrastructure.server;

import com.google.common.annotations.VisibleForTesting;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.NettyServerRouter;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerHandshakeHandler;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.security.sasl.plaintext.PlainTextSaslNettyServer;
import org.corfudb.security.tls.SslContextConstructor;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Version;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.security.sasl.SaslException;
import java.util.Optional;
import java.util.regex.Pattern;

public class NettyServerManager {

    private NettyServerManager() {
        //prevent creating instances
    }

    @Builder
    @Slf4j
    public static class NettyServerInstance {
        @NonNull
        private final ChannelFuture channelFuture;

        /**
         * Wait on Corfu Server Channel until it closes.
         */
        public void waitAndListen() {
            // Wait on it to close.
            getChannel().closeFuture().syncUninterruptibly();
        }

        public void close() {
            getChannel().close().syncUninterruptibly();
        }

        private Channel getChannel() {
            return channelFuture.channel();
        }
    }

    @Builder
    @Slf4j
    public static class NettyServerConfigurator {
        @NonNull
        private final ServerContext serverContext;
        @NonNull
        private final NettyServerRouter router;

        /**
         * Start the Corfu Server by listening on the specified port.
         */
        public NettyServerInstance start() {
            ChannelFuture channelFuture = bindServer(serverContext.getBossGroup(),
                    serverContext.getWorkerGroup(),
                    this::configureBootstrapOptions,
                    serverContext,
                    router,
                    (String) serverContext.getServerConfig().get("--address"),
                    Integer.parseInt((String) serverContext.getServerConfig().get("<port>"))
            );

            return NettyServerInstance.builder()
                    .channelFuture(channelFuture)
                    .build();
        }

        /**
         * Configure server bootstrap per-channel options, such as TCP options, etc.
         *
         * @param bootstrap The {@link ServerBootstrap} to be configured.
         */
        @VisibleForTesting
        public void configureBootstrapOptions(@Nonnull ServerBootstrap bootstrap) {
            bootstrap.option(ChannelOption.SO_BACKLOG, 100)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        /**
         * Bind the Corfu server to the given {@code port} using the provided
         * {@code channelType}. It is the callers' responsibility to shutdown the
         * {@link EventLoopGroup}s. For implementations which listen on multiple ports,
         * {@link EventLoopGroup}s may be reused.
         *
         * @param bossGroup           The "boss" {@link EventLoopGroup} which services incoming
         *                            connections.
         * @param workerGroup         The "worker" {@link EventLoopGroup} which services incoming
         *                            requests.
         * @param bootstrapConfigurer A {@link BootstrapConfigurer} which will receive the
         *                            {@link ServerBootstrap} to set options.
         * @param context             A {@link ServerContext} which will be used to configure
         *                            the server.
         * @param router              A {@link NettyServerRouter} which will process incoming
         *                            messages.
         * @param port                The port the {@link ServerChannel} will be created on.
         * @return A {@link ChannelFuture} which can be used to wait for the server to be shutdown.
         */
        @VisibleForTesting
        public ChannelFuture bindServer(@Nonnull EventLoopGroup bossGroup,
                                         @Nonnull EventLoopGroup workerGroup,
                                         @Nonnull BootstrapConfigurer bootstrapConfigurer,
                                         @Nonnull ServerContext context,
                                         @Nonnull NettyServerRouter router,
                                         String address,
                                         int port) {
            try {
                ServerBootstrap bootstrap = new ServerBootstrap();
                bootstrap.group(bossGroup, workerGroup)
                        .channel(context.getChannelImplementation().getServerChannelClass());
                bootstrapConfigurer.configure(bootstrap);

                bootstrap.childHandler(getServerChannelInitializer(context, router));
                boolean bindToAllInterfaces =
                        Optional.ofNullable(context.getServerConfig(Boolean.class, "--bind-to-all-interfaces"))
                                .orElse(false);
                if (bindToAllInterfaces) {
                    log.info("Corfu Server listening on all interfaces on port:{}", port);
                    return bootstrap.bind(port).sync();
                } else {
                    log.info("Corfu Server listening on {}:{}", address, port);
                    return bootstrap.bind(address, port).sync();
                }
            } catch (InterruptedException ie) {
                throw new IllegalStateException(ie);
            }
        }

        /**
         * Obtain a {@link ChannelInitializer} which initializes the channel pipeline
         * for a new {@link ServerChannel}.
         *
         * @param context The {@link ServerContext} to use.
         * @param router  The {@link NettyServerRouter} to initialize the channel with.
         * @return A {@link ChannelInitializer} to initialize the channel.
         */
        private ChannelInitializer<Channel> getServerChannelInitializer(
                @Nonnull ServerContext context, @Nonnull NettyServerRouter router) {

            // Generate the initializer.
            return new ChannelInitializer<Channel>() {

                @Override
                protected void initChannel(@Nonnull Channel ch) throws Exception {
                    setupEncryptionPipeline(ch);
                    setupPipeline(ch);
                }

                private void setupEncryptionPipeline(@Nonnull Channel ch) {
                    // Security Initialization
                    boolean tlsEnabled = context.getServerConfig(Boolean.class, "--enable-tls");
                    boolean tlsMutualAuthEnabled = context.getServerConfig(Boolean.class,
                            "--enable-tls-mutual-auth");

                    // If TLS is enabled, setup the encryption pipeline.
                    if (tlsEnabled) {
                        String[] enabledTlsCipherSuites = getTlsCiphers();
                        String[] enabledTlsProtocols = getTlsProtocols();
                        SslContext sslContext = setupSsl();

                        SSLEngine engine = sslContext.newEngine(ch.alloc());
                        engine.setEnabledCipherSuites(enabledTlsCipherSuites);
                        engine.setEnabledProtocols(enabledTlsProtocols);
                        if (tlsMutualAuthEnabled) {
                            engine.setNeedClientAuth(true);
                        }
                        ch.pipeline().addLast("ssl", new SslHandler(engine));
                    }
                }

                private void setupPipeline(@Nonnull Channel ch) throws SaslException {
                    // Add/parse a length field
                    boolean saslPlainTextAuth = context.getServerConfig(Boolean.class,
                            "--enable-sasl-plain-text-auth");

                    ch.pipeline().addLast(new LengthFieldPrepender(4));
                    ch.pipeline().addLast(getLengthFieldBasedFrameDecoder());
                    // If SASL authentication is requested, perform a SASL plain-text auth.
                    if (saslPlainTextAuth) {
                        ch.pipeline().addLast("sasl/plain-text", new PlainTextSaslNettyServer());
                    }
                    // Transform the framed message into a Corfu message.
                    ch.pipeline().addLast(new NettyCorfuMessageDecoder());
                    ch.pipeline().addLast(new NettyCorfuMessageEncoder());
                    ch.pipeline().addLast(getHandshakeHandler());
                    // Route the message to the server class.
                    ch.pipeline().addLast(router);
                }

                private LengthFieldBasedFrameDecoder getLengthFieldBasedFrameDecoder() {
                    return new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4);
                }

                private ServerHandshakeHandler getHandshakeHandler() {
                    return new ServerHandshakeHandler(
                            context.getNodeId(),
                            getCorfuVersion(),
                            context.getServerConfig(String.class, "--HandshakeTimeout")
                    );
                }

                private String getCorfuVersion() {
                    return Version.getVersionString() +
                            "(" + GitRepositoryState.getRepositoryState().commitIdAbbrev + ")";
                }

                private String[] getTlsCiphers() {
                    // Get the TLS cipher suites to enable
                    String[] enabledTlsCipherSuites;
                    String ciphs = context.getServerConfig(String.class, "--tls-ciphers");
                    if (ciphs != null) {
                        enabledTlsCipherSuites = Pattern.compile(",")
                                .splitAsStream(ciphs)
                                .map(String::trim)
                                .toArray(String[]::new);
                    } else {
                        enabledTlsCipherSuites = new String[]{};
                    }
                    return enabledTlsCipherSuites;
                }

                private String[] getTlsProtocols() {
                    String[] enabledTlsProtocols;// Get the TLS protocols to enable
                    String protos = context.getServerConfig(String.class, "--tls-protocols");
                    if (protos != null) {
                        enabledTlsProtocols = Pattern.compile(",")
                                .splitAsStream(protos)
                                .map(String::trim)
                                .toArray(String[]::new);
                    } else {
                        enabledTlsProtocols = new String[]{};
                    }
                    return enabledTlsProtocols;
                }

                private SslContext setupSsl() {
                    try {
                        return SslContextConstructor.constructSslContext(true,
                                context.getServerConfig(String.class, "--keystore"),
                                context.getServerConfig(String.class, "--keystore-password-file"),
                                context.getServerConfig(String.class, "--truststore"),
                                context.getServerConfig(String.class, "--truststore-password-file")
                        );
                    } catch (SSLException e) {
                        log.error("Could not build the SSL context", e);
                        throw new IllegalStateException("Couldn't build the SSL context", e);
                    }
                }
            };
        }

        /**
         * A functional interface for receiving and configuring a {@link ServerBootstrap}.
         */
        @FunctionalInterface
        public interface BootstrapConfigurer {

            /**
             * Configure a {@link ServerBootstrap}.
             *
             * @param serverBootstrap The {@link ServerBootstrap} to configure.
             */
            void configure(ServerBootstrap serverBootstrap);
        }
    }
}
