package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.configuration.ServerConfiguration;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.security.sasl.plaintext.PlainTextSaslNettyServer;
import org.corfudb.security.tls.SslContextConstructor;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.NetworkUtils;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;


/**
 * Created by zlokhandwala on 2019-05-06.
 */
@Slf4j
public class CorfuServerNode implements AutoCloseable {

    @Getter
    private final ServerContext serverContext;

    @Getter
    private final Map<Class, AbstractServer> serverMap;

    @Getter
    private final NettyServerRouter router;

    // This flag makes the closing of the CorfuServer idempotent.
    private final AtomicBoolean close;

    private ChannelFuture bindFuture;

    /**
     * Corfu Server initialization.
     *
     * @param serverContext Initialized Server Context.
     */
    public CorfuServerNode(@Nonnull ServerContext serverContext) {
        this(serverContext,
                ImmutableMap.<Class, AbstractServer>builder()
                        .put(BaseServer.class, new BaseServer(serverContext))
                        .put(SequencerServer.class, new SequencerServer(serverContext))
                        .put(LayoutServer.class, new LayoutServer(serverContext))
                        .put(LogUnitServer.class, new LogUnitServer(serverContext))
                        .put(ManagementServer.class, new ManagementServer(serverContext,
                                new ManagementServer.ManagementServerInitializer()))
                        .build()
        );
    }

    /**
     * Corfu Server initialization.
     *
     * @param serverContext Initialized Server Context.
     * @param serverMap     Server Map with all components.
     */
    public CorfuServerNode(@Nonnull ServerContext serverContext,
                           @Nonnull ImmutableMap<Class, AbstractServer> serverMap) {
        this.serverContext = serverContext;
        this.serverMap = serverMap;
        router = new NettyServerRouter(serverMap.values().asList(), serverContext);
        this.serverContext.setServerRouter(router);
        // If the node is started in the single node setup and was bootstrapped,
        // set the server epoch as well.
        if(serverContext.isSingleNodeSetup() && serverContext.getCurrentLayout() != null){
            serverContext.setServerEpoch(serverContext.getCurrentLayout().getEpoch(),
                    router);
        }
        this.close = new AtomicBoolean(false);
    }

    /**
     * Start the Corfu Server by listening on the specified port.
     */
    public ChannelFuture start() {
        String host = serverContext.getConfiguration().getHostAddress();
        String interfaceToBind = serverContext.getConfiguration().getNetworkInterface();
        if (interfaceToBind != null) {
            host = NetworkUtils.getAddressFromInterfaceName(interfaceToBind);
        }
        bindFuture = bindServer(serverContext.getWorkerGroup(),
                this::configureBootstrapOptions,
                serverContext,
                router,
                host,
                serverContext.getConfiguration().getServerPort());

        return bindFuture.syncUninterruptibly();
    }

    /**
     * Wait on Corfu Server Channel until it closes.
     */
    public void startAndListen() {
        // Wait on it to close.
        this.start().channel().closeFuture().syncUninterruptibly();
    }

    /**
     * Closes the currently running corfu server.
     */
    @Override
    public void close() {

        if (!close.compareAndSet(false, true)) {
            log.trace("close: Server already shutdown");
            return;
        }

        log.info("close: Shutting down Corfu server and cleaning resources");
        serverContext.close();
        if (bindFuture != null) {
            bindFuture.channel().close().syncUninterruptibly();
        }

        // A executor service to create the shutdown threads
        // plus name the threads correctly.
        final ExecutorService shutdownService = Executors.newFixedThreadPool(serverMap.size(),
                new ServerThreadFactory("CorfuServer-shutdown-",
                        new ServerThreadFactory.ExceptionHandler()));

        // Turn into a list of futures on the shutdown, returning
        // generating a log message to inform of the result.
        CompletableFuture[] shutdownFutures = serverMap.values().stream()
                .map(server -> CompletableFuture.runAsync(() -> {
                    try {
                        log.info("close: Shutting down {}", server.getClass().getSimpleName());
                        server.shutdown();
                        log.info("close: Cleanly shutdown {}", server.getClass().getSimpleName());
                    } catch (Exception e) {
                        log.error("close: Failed to cleanly shutdown {}",
                                server.getClass().getSimpleName(), e);
                    }
                }, shutdownService))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(shutdownFutures).join();
        shutdownService.shutdown();
        log.info("close: Server shutdown and resources released");
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
     * @param context             A {@link ServerContext} which will be used to configure
     *                            the server.
     * @param router              A {@link NettyServerRouter} which will process incoming
     *                            messages.
     * @param port                The port the {@link ServerChannel} will be created on.
     * @return A {@link ChannelFuture} which can be used to wait for the server to be shutdown.
     */
    public ChannelFuture bindServer(@Nonnull EventLoopGroup workerGroup,
                                    @Nonnull BootstrapConfigurer bootstrapConfigurer,
                                    @Nonnull ServerContext context,
                                    @Nonnull NettyServerRouter router,
                                    String address,
                                    int port) {
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(workerGroup)
                    .channel(context.getConfiguration().getChannelImplementation().getServerChannelClass());
            bootstrapConfigurer.configure(bootstrap);

            bootstrap.childHandler(getServerChannelInitializer(context, router));
            boolean bindToAllInterfaces = context.getConfiguration().getBindToAllInterfaces();
            if (bindToAllInterfaces) {
                log.info("Corfu Server listening on all interfaces on port:{}", port);
                return bootstrap.bind(port).sync();
            } else {
                log.info("Corfu Server listening on {}:{}", address, port);
                return bootstrap.bind(address, port).sync();
            }
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
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
     * for a new {@link ServerChannel}.
     *
     * @param context The {@link ServerContext} to use.
     * @param router  The {@link NettyServerRouter} to initialize the channel with.
     * @return A {@link ChannelInitializer} to initialize the channel.
     */
    private static ChannelInitializer getServerChannelInitializer(@Nonnull ServerContext context,
                                                                  @Nonnull NettyServerRouter router) {

        // Generate the initializer.
        return new ChannelInitializer() {
            @Override
            protected void initChannel(@Nonnull Channel ch) throws Exception {

                // Security variables
                final SslContext sslContext;
                final String[] enabledTlsProtocols;
                final String[] enabledTlsCipherSuites;

                // Security Initialization
                ServerConfiguration conf = context.getConfiguration();
                boolean tlsEnabled = conf.isTlsEnabled();
                boolean tlsMutualAuthEnabled = conf.getEnableTlsMutualAuth();

                if (tlsEnabled) {
                    // Get the TLS cipher suites to enable
                    String ciphs = conf.getTlsCiphers();
                    if (ciphs != null) {
                        enabledTlsCipherSuites = Pattern.compile(",")
                                .splitAsStream(ciphs)
                                .map(String::trim)
                                .toArray(String[]::new);
                    } else {
                        enabledTlsCipherSuites = new String[]{};
                    }

                    // Get the TLS protocols to enable
                    String protos = conf.getTlsProtocols();
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
                                conf.getKeystore(),
                                conf.getKeystorePasswordFile(),
                                conf.getTruststore(),
                                conf.getTruststorePasswordFile());
                    } catch (SSLException e) {
                        log.error("Could not build the SSL context", e);
                        throw new RuntimeException("Couldn't build the SSL context", e);
                    }
                } else {
                    enabledTlsCipherSuites = new String[]{};
                    enabledTlsProtocols = new String[]{};
                    sslContext = null;
                }

                boolean saslPlainTextAuth = conf.getEnableSaslPlainTextAuth();

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
                // Add/parse a length field
                ch.pipeline().addLast(new LengthFieldPrepender(4));
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer
                        .MAX_VALUE, 0, 4,
                        0, 4));
                // If SASL authentication is requested, perform a SASL plain-text auth.
                if (saslPlainTextAuth) {
                    ch.pipeline().addLast("sasl/plain-text", new
                            PlainTextSaslNettyServer());
                }
                // Transform the framed message into a Corfu message.
                ch.pipeline().addLast(new NettyCorfuMessageDecoder());
                ch.pipeline().addLast(new NettyCorfuMessageEncoder());
                ch.pipeline().addLast(new ServerHandshakeHandler(context.getNodeId(),
                        GitRepositoryState.getCorfuSourceCodeVersion(),
                        conf.getHandshakeTimeout()));
                // Route the message to the server class.
                ch.pipeline().addLast(router);
            }
        };
    }
}
