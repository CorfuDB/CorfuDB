package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.ServerChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.prometheus.client.exporter.HTTPServer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.corfudb.common.config.ConfigParamNames;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider.MeterRegistryInitializer;
import org.corfudb.infrastructure.ManagementServer.ManagementServerInitializer;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.HttpServerInitializer;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Layout;
import org.corfudb.security.sasl.plaintext.PlainTextSaslNettyServer;
import org.corfudb.security.tls.SslContextConstructor;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.KeyStoreConfig;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.TrustStoreConfig;
import org.corfudb.util.GitRepositoryState;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static org.corfudb.common.util.URLUtils.getVersionFormattedHostAddress;
import static org.corfudb.infrastructure.CorfuServerCmdLine.HEALTH_PORT_PARAM;
import static org.corfudb.infrastructure.CorfuServerCmdLine.METRICS_PORT_PARAM;


/**
 * Created by zlokhandwala on 2019-05-06.
 */
@Slf4j
public class CorfuServerNode implements AutoCloseable {

    @Getter
    private final ServerContext serverContext;

    @Getter
    private final Map<Class<?>, AbstractServer> serverMap;

    @Getter
    private final NettyServerRouter router;

    // This flag makes the closing of the CorfuServer idempotent.
    private final AtomicBoolean close;

    private ChannelFuture bindFuture;

    private ChannelFuture httpServerFuture;

    private HTTPServer metricsServer;

    /**
     * Corfu Server initialization.
     *
     * @param serverContext Initialized Server Context.
     */
    public CorfuServerNode(@Nonnull ServerContext serverContext) {
        this(serverContext, ImmutableMap.<Class<?>, AbstractServer>builder()
                .put(BaseServer.class, new BaseServer(serverContext))
                .put(SequencerServer.class, new SequencerServer(serverContext))
                .put(LayoutServer.class, new LayoutServer(serverContext))
                .put(LogUnitServer.class, new LogUnitServer(serverContext))
                .put(ManagementServer.class, new ManagementServer(serverContext, new ManagementServerInitializer()))
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
                           @Nonnull ImmutableMap<Class<?>, AbstractServer> serverMap) {
        this.serverContext = serverContext;
        this.serverMap = serverMap;
        this.router = new NettyServerRouter(serverMap.values().asList(), serverContext);
        this.serverContext.setServerRouter(router);
        // If the node is started in the single node setup and was bootstrapped,
        // set the server epoch as well.
        Optional<Layout> maybeCurrentLayout = serverContext.findCurrentLayout();

        maybeCurrentLayout.ifPresent(currentLayout -> {
            if (serverContext.isSingleNodeSetup()) {
                serverContext.setServerEpoch(currentLayout.getEpoch(), router);
            }
        });

        this.close = new AtomicBoolean(false);
    }

    /**
     * Start the Corfu Server by listening on the specified port.
     */
    public ChannelFuture start() {
        final int corfuPort = Integer.parseInt((String) serverContext.getServerConfig().get("<port>"));
        bindFuture = bindServer(
                serverContext.getWorkerGroup(),
                this::configureBootstrapOptions,
                serverContext,
                router,
                (String) serverContext.getServerConfig().get("--address"),
                corfuPort
        );

        Optional<Integer> maybeHealthPort = Optional
                .ofNullable(serverContext.getServerConfig().get(HEALTH_PORT_PARAM))
                .map(healthApiPort -> Integer.parseInt(healthApiPort.toString()));

        maybeHealthPort.ifPresent(healthApiPort -> {
            if (healthApiPort == corfuPort) {
                log.warn("Port unification is not currently supported. Health server will not be started.");
            } else {
                httpServerFuture = bindHttpServer(
                        serverContext.getWorkerGroup(),
                        this::configureBootstrapOptions,
                        serverContext,
                        healthApiPort
                );
            }
        });

        Optional<Integer> maybeMetricsPort = Optional
                .ofNullable(serverContext.getServerConfig().get(METRICS_PORT_PARAM))
                .map(metricsApiPort -> Integer.parseInt(metricsApiPort.toString()));

        maybeMetricsPort.ifPresent(metricsApiPort -> {
            if (metricsApiPort == corfuPort) {
                log.warn("Port unification is not currently supported. Metrics server will not be started.");
            } else {
                PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
                MeterRegistryInitializer.getMeterRegistry().add(prometheusRegistry);

                try {
                    metricsServer = new HTTPServer.Builder()
                            .withPort(metricsApiPort)
                            .withRegistry(prometheusRegistry.getPrometheusRegistry())
                            .build();
                    log.info("Metrics server run on: " + metricsServer.getPort() + " port");
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        });

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

        if (bindFuture != null) {
            bindFuture.channel().close().syncUninterruptibly();
        }
        if (httpServerFuture != null) {
            httpServerFuture.channel().close().syncUninterruptibly();
        }
        serverContext.close();

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
        // If it's the server who initialized the registry - shut it down
        MeterRegistryProvider.getMetricType().ifPresent(type -> {
            if (type == MeterRegistryProvider.MetricType.SERVER) {
                MeterRegistryProvider.close();
            }
        });
        HealthMonitor.shutdown();
        IOUtils.closeQuietly(metricsServer, ex -> {});
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
                log.info("Corfu Server listening on {}:{}",
                        getVersionFormattedHostAddress(address), port);
                return bootstrap.bind(getVersionFormattedHostAddress(address), port).sync();
            }
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
    }

    public ChannelFuture bindHttpServer(@Nonnull EventLoopGroup workerGroup,
                                        @Nonnull BootstrapConfigurer bootstrapConfigurer,
                                        @Nonnull ServerContext context, int port) {
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(workerGroup)
                    .channel(context.getChannelImplementation().getServerChannelClass());
            bootstrapConfigurer.configure(bootstrap);
            bootstrap.childHandler(new HttpServerInitializer());
            boolean bindToAllInterfaces =
                    Optional.ofNullable(context.getServerConfig(Boolean.class, "--bind-to-all-interfaces"))
                            .orElse(false);
            String address = (String) serverContext.getServerConfig().get("--address");

            if (bindToAllInterfaces) {
                log.info("Corfu Http Server listening on all interfaces on port:{}", port);
                return bootstrap.bind(port).sync();
            } else {
                log.info("Corfu Http Server listening on {}:{}",
                        getVersionFormattedHostAddress(address), port);
                return bootstrap.bind(getVersionFormattedHostAddress(address), port).sync();
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
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                        300 * 1024, 500 * 1024))
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
    private static ChannelInitializer<Channel> getServerChannelInitializer(
            @Nonnull ServerContext context, @Nonnull NettyServerRouter router) {

        return new CorfuChannelInitializer(context, router);
    }

    private static class CorfuChannelInitializer extends ChannelInitializer<Channel> {
        private final ServerContext context;
        private final NettyServerRouter router;

        public CorfuChannelInitializer(ServerContext context, NettyServerRouter router) {
            this.context = context;
            this.router = router;
        }

        @Override
        protected void initChannel(@Nonnull Channel ch) throws Exception {

            // Security variables
            final SslContext sslContext;
            final String[] enabledTlsProtocols;
            final String[] enabledTlsCipherSuites;

            // Security Initialization
            boolean tlsEnabled = context.getServerConfig(Boolean.class, "--enable-tls");
            boolean tlsMutualAuthEnabled = context.getServerConfig(Boolean.class, "--enable-tls-mutual-auth");

            if (tlsEnabled) {
                // Get the TLS cipher suites to enable
                String ciphs = context.getServerConfig(String.class, "--tls-ciphers");
                if (ciphs != null) {
                    enabledTlsCipherSuites = Pattern.compile(",")
                            .splitAsStream(ciphs)
                            .map(String::trim)
                            .toArray(String[]::new);
                } else {
                    enabledTlsCipherSuites = new String[]{};
                }

                // Get the TLS protocols to enable
                String protos = context.getServerConfig(String.class, "--tls-protocols");
                if (protos != null) {
                    enabledTlsProtocols = Pattern.compile(",")
                            .splitAsStream(protos)
                            .map(String::trim)
                            .toArray(String[]::new);
                } else {
                    enabledTlsProtocols = new String[]{};
                }

                try {
                    KeyStoreConfig keyStoreConfig = KeyStoreConfig.from(
                            context.getServerConfig(String.class, ConfigParamNames.KEY_STORE),
                            context.getServerConfig(String.class, ConfigParamNames.KEY_STORE_PASS_FILE)
                    );

                    Path certExpiryFile = context
                            .<String>getServerConfig(ConfigParamNames.DISABLE_CERT_EXPIRY_CHECK_FILE)
                            .map(Paths::get)
                            .orElse(TrustStoreConfig.DEFAULT_DISABLE_CERT_EXPIRY_CHECK_FILE);

                    log.trace("getServerChannelInitializer: certExpiryFile path is {}, isCertExpiryCheckEnabled is {}.",
                            certExpiryFile, !Files.exists(certExpiryFile));

                    TrustStoreConfig trustStoreConfig = TrustStoreConfig.from(
                            context.getServerConfig(String.class, ConfigParamNames.TRUST_STORE),
                            context.getServerConfig(String.class, ConfigParamNames.TRUST_STORE_PASS_FILE),
                            certExpiryFile
                    );

                    sslContext = SslContextConstructor.constructSslContext(
                            true, keyStoreConfig, trustStoreConfig
                    );
                } catch (SSLException e) {
                    log.error("Could not build the SSL context", e);
                    throw new RuntimeException("Couldn't build the SSL context", e);
                }
            } else {
                enabledTlsCipherSuites = new String[]{};
                enabledTlsProtocols = new String[]{};
                sslContext = null;
            }

            Boolean saslPlainTextAuth = context.getServerConfig(Boolean.class,
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
            // Add/parse a length field
            ch.pipeline().addLast(new LengthFieldPrepender(4));
            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
            // If SASL authentication is requested, perform a SASL plain-text auth.
            if (saslPlainTextAuth) {
                ch.pipeline().addLast("sasl/plain-text", new PlainTextSaslNettyServer());
            }
            // Transform the framed message into a Corfu message.
            ch.pipeline().addLast(new NettyCorfuMessageDecoder());
            ch.pipeline().addLast(new NettyCorfuMessageEncoder());

            ServerHandshakeHandler serverHandshakeHandler = new ServerHandshakeHandler(
                    context.getNodeId(),
                    GitRepositoryState.getCorfuSourceCodeVersion(),
                    context.getServerConfig(String.class, "--HandshakeTimeout")
            );
            ch.pipeline().addLast(serverHandshakeHandler);

            // Route the message to the server class.
            ch.pipeline().addLast(router);
        }
    }
}
