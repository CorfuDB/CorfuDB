package org.corfudb.infrastructure.logreplication.transport.sample;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.security.sasl.plaintext.PlainTextSaslNettyServer;
import org.corfudb.security.tls.SslContextConstructor;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationServerRouter;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;


import javax.annotation.Nonnull;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

@Slf4j
public class NettyLogReplicationServerChannelAdapter extends IServerChannelAdapter {

    private CorfuNettyServerChannel nettyServerChannel;

    private ChannelFuture bindFuture;

    private final int port;

    private CompletableFuture<Boolean> connectionEnded;

    public NettyLogReplicationServerChannelAdapter(ServerContext serverContext, LogReplicationServerRouter router) {
        super(serverContext, router);
        this.port = Integer.parseInt((String) serverContext.getServerConfig().get("<port>"));
        this.nettyServerChannel = new CorfuNettyServerChannel(this);
    }

    // ================== IServerChannelAdapter ==================

    @Override
    public void send(CorfuMessage msg) {
        nettyServerChannel.sendResponse(msg);
    }

    @Override
    public CompletableFuture<Boolean> start() {
        startServer().channel().closeFuture().syncUninterruptibly();
        connectionEnded = new CompletableFuture<>();
        return connectionEnded;
    }

    @Override
    public void stop() {
        bindFuture.channel().close().syncUninterruptibly();
        if (connectionEnded != null) {
            connectionEnded.complete(true);
        }
    }

    // ==========================================================

    /**
     * Bind the Corfu server to the given {@code port} using the provided
     * {@code channelType}. It is the callers' responsibility to shutdown the
     * {@link EventLoopGroup}s. For implementations which listen on multiple ports,
     * {@link EventLoopGroup}s may be reused.
     *
     * @param workerGroup         The "worker" {@link EventLoopGroup} which services incoming
     *                            requests.
     * @param bootstrapConfigurer A {@link BootstrapConfigurer} which will receive the
     *                            {@link ServerBootstrap} to set options.
     * @param port                The port will be created on.
     * @return A {@link ChannelFuture} which can be used to wait for the server to be shutdown.
     */
    public ChannelFuture bindServer(@Nonnull EventLoopGroup workerGroup,
                                    @Nonnull BootstrapConfigurer bootstrapConfigurer,
                                    String address,
                                    int port) {
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(workerGroup)
                    .channel(getServerContext().getChannelImplementation().getServerChannelClass());
            bootstrapConfigurer.configure(bootstrap);

            bootstrap.childHandler(getServerChannelInitializer());
            boolean bindToAllInterfaces =
                    Optional.ofNullable(getServerContext().getServerConfig(Boolean.class, "--bind-to-all-interfaces"))
                            .orElse(false);
            if (bindToAllInterfaces) {
                log.info("Log Replication Server listening on all interfaces on port:{}", port);
                return bootstrap.bind(port).sync();
            } else {
                log.info("Log Replication Server listening on {}:{}", address, port);
                return bootstrap.bind(address, port).sync();
            }
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
    }


    /**
     * Start the Corfu Replication Server by listening on the specified port.
     */
    private ChannelFuture startServer() {
        bindFuture = bindServer(getServerContext().getWorkerGroup(),
                this::configureBootstrapOptions,
                (String) getServerContext().getServerConfig().get("--address"),
                port);

        return bindFuture.syncUninterruptibly();
    }

    /**
     * Configure server bootstrap per-channel options, such as TCP options, etc.
     *
     * @param bootstrap The {@link ServerBootstrap} to be configured.
     */
    public void configureBootstrapOptions(@Nonnull ServerBootstrap bootstrap) {
        bootstrap.option(ChannelOption.SO_BACKLOG, 100)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    }

    /**
     * Obtain a {@link ChannelInitializer} which initializes the channel pipeline
     *
     * @return A {@link ChannelInitializer} to initialize the channel.
     */
    private ChannelInitializer getServerChannelInitializer() {

        // Generate the initializer.
        return new ChannelInitializer() {
            @Override
            protected void initChannel(@Nonnull Channel ch) throws Exception {
                // Security variables
                final SslContext sslContext;
                final String[] enabledTlsProtocols;
                final String[] enabledTlsCipherSuites;

                // Security Initialization
                Boolean tlsEnabled = getServerContext().getServerConfig(Boolean.class, "--enable-tls");
                Boolean tlsMutualAuthEnabled = getServerContext().getServerConfig(Boolean.class,
                        "--enable-tls-mutual-auth");
                if (tlsEnabled) {
                    // Get the TLS cipher suites to enable
                    String ciphs = getServerContext().getServerConfig(String.class, "--tls-ciphers");
                    if (ciphs != null) {
                        enabledTlsCipherSuites = Pattern.compile(",")
                                .splitAsStream(ciphs)
                                .map(String::trim)
                                .toArray(String[]::new);
                    } else {
                        enabledTlsCipherSuites = new String[]{};
                    }

                    // Get the TLS protocols to enable
                    String protos = getServerContext().getServerConfig(String.class, "--tls-protocols");
                    if (protos != null) {
                        enabledTlsProtocols = Pattern.compile(",")
                                .splitAsStream(protos)
                                .map(String::trim)
                                .toArray(String[]::new);
                    } else {
                        enabledTlsProtocols = new String[]{};
                    }

                    try {
                        sslContext = SslContextConstructor.constructSslContext(true,
                                getServerContext().getServerConfig(String.class, "--keystore"),
                                getServerContext().getServerConfig(String.class, "--keystore-password-file"),
                                getServerContext().getServerConfig(String.class, "--truststore"),
                                getServerContext().getServerConfig(String.class,
                                        "--truststore-password-file"));
                    } catch (SSLException e) {
                        log.error("Could not build the SSL context", e);
                        throw new RuntimeException("Couldn't build the SSL context", e);
                    }
                } else {
                    enabledTlsCipherSuites = new String[]{};
                    enabledTlsProtocols = new String[]{};
                    sslContext = null;
                }

                Boolean saslPlainTextAuth = getServerContext().getServerConfig(Boolean.class,
                        "--enable-sasl-plain-text-auth");

                // If TLS is enabled, setup the encryption pipeline.
                if (tlsEnabled) {
                    SSLEngine engine = sslContext.newEngine(ch.alloc());
                    engine.setEnabledCipherSuites(enabledTlsCipherSuites);
                    engine.setEnabledProtocols(enabledTlsProtocols);
                    if (tlsMutualAuthEnabled) {
                        engine.setNeedClientAuth(true);
                    }
                    ch.pipeline().addLast("ssl", new SslHandler(engine));
                }

                // If SASL authentication is requested, perform a SASL plain-text auth.
                if (saslPlainTextAuth) {
                    ch.pipeline().addLast("sasl/plain-text", new
                            PlainTextSaslNettyServer());
                }
                // Transform the framed message into a Corfu message.
                ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
                ch.pipeline().addLast(new ProtobufDecoder(CorfuMessage.getDefaultInstance()));
                ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
                ch.pipeline().addLast(new ProtobufEncoder());

                // Route the message to the server class.
                ch.pipeline().addLast(nettyServerChannel);
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
