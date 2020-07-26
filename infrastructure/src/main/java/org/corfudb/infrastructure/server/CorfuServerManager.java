package org.corfudb.infrastructure.server;

import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.MetricsServer;
import org.corfudb.common.metrics.servers.PrometheusMetricsServer;
import org.corfudb.common.util.LoggerUtil;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ManagementServer;
import org.corfudb.infrastructure.NettyServerRouter;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.server.CorfuServerStateGraph.CorfuServerState;
import org.corfudb.infrastructure.server.NettyServerManager.NettyServerConfigurator;
import org.corfudb.infrastructure.server.NettyServerManager.NettyServerInstance;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Utils;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

@Builder
@Slf4j
public class CorfuServerManager {

    @NonNull
    private final CorfuServerConfigurator configurator;
    @NonNull
    private final Optional<CorfuServerInstance> instance;
    @NonNull
    private final CorfuServerStateGraph stateGraph = new CorfuServerStateGraph();

    @Getter
    @NonNull
    private final CorfuServerState currentState;

    public CorfuServerManager startServer(CorfuServerStateMachine serverStateMachine) {
        return ifLegalTransition(CorfuServerState.START, nextState -> {
            setupMetrics();

            ServerContext serverContext = new ServerContext(configurator.opts);

            LogUnitServer logUnitServer = new LogUnitServer(serverContext, serverStateMachine);

            ImmutableMap<Class<? extends AbstractServer>, AbstractServer> serverMap = ImmutableMap
                    .<Class<? extends AbstractServer>, AbstractServer>builder()
                    .put(BaseServer.class, new BaseServer(serverContext, serverStateMachine))
                    .put(SequencerServer.class, new SequencerServer(serverContext))
                    .put(LayoutServer.class, new LayoutServer(serverContext))
                    .put(LogUnitServer.class, logUnitServer)
                    .put(ManagementServer.class, new ManagementServer(serverContext))
                    .build();

            NettyServerRouter router = new NettyServerRouter(
                    serverMap.values().asList(), serverContext
            );

            serverContext.setServerRouter(router);

            if (serverContext.isSingleNodeSetup() && serverContext.getCurrentLayout() != null) {
                serverContext.setServerEpoch(serverContext.getCurrentLayout().getEpoch(), router);
            }

            NettyServerInstance nettyServer = NettyServerConfigurator.builder()
                    .serverContext(serverContext)
                    .router(router)
                    .build()
                    .start();

            CorfuServerInstance currentInstance = CorfuServerInstance.builder()
                    .serverContext(serverContext)
                    .serverMap(serverMap)
                    .router(router)
                    .nettyServer(nettyServer)
                    .logUnitServer(logUnitServer)
                    .build();

            CorfuServerManager serverManager = CorfuServerManager.builder()
                    .configurator(configurator)
                    .currentState(nextState)
                    .instance(Optional.of(currentInstance))
                    .build();

            serverManager.setupShutdownHook();

            return serverManager;
        });
    }

    public CorfuServerManager stopAndCleanServer() {
        CorfuServerManager newServer = stopServer();

        if (!stateGraph.isTransitionLegal(currentState, CorfuServerState.STOP)) {
            return newServer;
        }

        if (!instance.isPresent()) {
            return newServer;
        }

        CorfuServerInstance server = instance.get();
        ServerContext serverContext = server.serverContext;

        File logPath = new File(serverContext.getServerConfig(String.class, "--log-path"));

        if (!serverContext.getServerConfig(Boolean.class, "--memory")) {
            Utils.clearDataFiles(logPath);
        }

        return newServer;
    }

    /**
     * Closes the currently running corfu server.
     */
    public CorfuServerManager stopServer() {
        log.info("close: Shutting down Corfu server and cleaning resources");
        return ifLegalTransition(CorfuServerState.STOP, nextState -> {
            if (!instance.isPresent()) {
                log.warn("Corfu server instance is not present");
                return this;
            }

            CorfuServerInstance server = instance.get();
            server.serverContext.close();
            server.nettyServer.close();
            // Turn into a list of futures on the shutdown, returning
            // generating a log message to inform of the result.
            List<CompletableFuture<Void>> shutdownFutures = server.serverMap
                    .values()
                    .stream()
                    .map(AbstractServer::shutdown)
                    .collect(Collectors.toList());

            CFUtils.allOf(shutdownFutures)
                    .exceptionally(ex -> {
                        log.warn("Failed during shutdown", ex);
                        return null;
                    })
                    .join();

            return CorfuServerManager.builder()
                    .instance(Optional.empty())
                    .currentState(nextState)
                    .configurator(configurator)
                    .build();
        });
    }

    private CorfuServerManager ifLegalTransition(
            CorfuServerState nextState, Function<CorfuServerState, CorfuServerManager> action) {
        if (!stateGraph.isTransitionLegal(currentState, nextState)) {
            log.warn("Illegal state transition. From: {}, to: {}", currentState, nextState);
            return this;
        }

        return action.apply(nextState);
    }

    /**
     * Generate metrics server config and start server.
     */
    private void setupMetrics() {
        PrometheusMetricsServer.Config config = PrometheusMetricsServer.Config.parse(configurator.opts);
        MetricsServer server = new PrometheusMetricsServer(config, ServerContext.getMetrics());
        server.start();
    }

    /**
     * Register shutdown handler
     */
    private void setupShutdownHook() {
        Thread shutdownThread = new Thread(this::shutdownHook);
        shutdownThread.setName("ShutdownThread");
        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }

    /**
     * Attempt to cleanly shutdown all the servers.
     */
    private void shutdownHook() {
        log.info("shutdown corfu server");

        stopServer();
        LoggerUtil.stopLogger();
    }

    public CorfuServerManager resetLogUnitServer() {
        return ifLegalTransition(CorfuServerState.STOP, nextState -> {
            instance.ifPresent(corfuServerInstance -> {
                corfuServerInstance.logUnitServer.getStreamLog().reset();
            });

            return this;
        });
    }

    @Builder
    public static class CorfuServerConfigurator {
        @NonNull
        private final Map<String, Object> opts;
    }

    @Builder
    public static class CorfuServerInstance {
        @NonNull
        private final ServerContext serverContext;
        @NonNull
        private final ImmutableMap<Class<? extends AbstractServer>, AbstractServer> serverMap;
        @NonNull
        private final NettyServerRouter router;
        @NonNull
        private final NettyServerInstance nettyServer;
        @NonNull
        private final LogUnitServer logUnitServer;
    }
}
