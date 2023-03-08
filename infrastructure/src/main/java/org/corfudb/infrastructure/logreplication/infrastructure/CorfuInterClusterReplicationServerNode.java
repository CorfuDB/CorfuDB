package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerThreadFactory;
import org.corfudb.infrastructure.logreplication.infrastructure.msgHandlers.LogReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkServerRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceServerRouter;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class manages the lifecycle of the transport layer server
 */
@Slf4j
public class CorfuInterClusterReplicationServerNode implements AutoCloseable {

    @Getter
    private final ServerContext serverContext;

    @Getter
    private final Map<Class, AbstractServer> serverMap;

    // This flag makes the closing of the CorfuServer idempotent.
    private final AtomicBoolean close;

    private LogReplicationServer logReplicationServer;

    private ScheduledExecutorService logReplicationServerRunner;

    // Error code required to detect an ungraceful shutdown.
    private static final int EXIT_ERROR_CODE = 100;

    // this flag makes the start/stop operation of transportLayerServer idempotent
    private AtomicBoolean serverStarted;

    private IServerChannelAdapter transportLayerServer;

    /**
     * Log Replication Server initialization.
     *
     * @param serverContext Initialized Server Context
     * @param serverMap Servers which help process/validate incoming requests
     */
    public CorfuInterClusterReplicationServerNode(@Nonnull ServerContext serverContext,
                                                  Map<Class, AbstractServer> serverMap) {

        this.serverContext = serverContext;

        this.logReplicationServer = (LogReplicationServer) serverMap.get(LogReplicationServer.class);

        this.serverMap = serverMap;

        this.close = new AtomicBoolean(false);

        this.serverStarted = new AtomicBoolean(false);
    }

    public void setRouterAndStartServer(Map<LogReplicationSession, LogReplicationSourceServerRouter> sessionToSourceServer,
                                        Map<LogReplicationSession, LogReplicationSinkServerRouter> sessionToSinkServer) {
        if (!serverStarted.get()) {
            createTransportServerAdapter(serverContext, sessionToSourceServer, sessionToSinkServer);
            setTransportServerInRouters(sessionToSourceServer, sessionToSinkServer);
            logReplicationServerRunner = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setNameFormat("replication-server-runner").build());
            // Start and listen to the server
            logReplicationServerRunner.submit(() -> this.startAndListen());
        } else {
            log.info("Server transport adapter is already running. Updating the router information");
            setTransportServerInRouters(sessionToSourceServer, sessionToSinkServer);
        }
    }

    private void createTransportServerAdapter(ServerContext serverContext,
                                              Map<LogReplicationSession, LogReplicationSourceServerRouter> sessionToSourceServer,
                                              Map<LogReplicationSession, LogReplicationSinkServerRouter> sessionToSinkServer) {

        LogReplicationPluginConfig config = new LogReplicationPluginConfig(serverContext.getPluginConfigFilePath());
        File jar = new File(config.getTransportAdapterJARPath());

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class adapter = Class.forName(config.getTransportServerClassCanonicalName(), true, child);
            this.transportLayerServer =  (IServerChannelAdapter) adapter.getDeclaredConstructor(ServerContext.class, Map.class, Map.class)
                    .newInstance(serverContext, sessionToSourceServer, sessionToSinkServer);
        } catch (Exception e) {
            log.error("Fatal error: Failed to create serverAdapter", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    /**
     * Set the transport layer server in new routers
     *
     * @param sessionToSourceRouter
     * @param sessionToSinkRouter
     */
    private void setTransportServerInRouters(Map<LogReplicationSession, LogReplicationSourceServerRouter> sessionToSourceRouter,
                                             Map<LogReplicationSession, LogReplicationSinkServerRouter> sessionToSinkRouter) {
        transportLayerServer.updateRouters(sessionToSourceRouter, sessionToSinkRouter);
        // set the server adapter in routers.
        if(!sessionToSourceRouter.isEmpty()) {
            sessionToSourceRouter.values().stream().filter(router -> router.getServerChannelAdapter() == null).forEach(router ->
                    router.setAdapter(transportLayerServer));
        }
        if (!sessionToSinkRouter.isEmpty()) {
            sessionToSinkRouter.values().stream().filter(router -> router.getServerAdapter() == null).forEach(router ->
                    router.setAdapter(transportLayerServer));
        }
    }

    /**
     * Wait on Corfu Server Channel until it closes.
     */
    private void startAndListen() {
        try {
            log.info("Starting server transport adapter ...");
            serverStarted.set(true);
            this.transportLayerServer.start().get();
        } catch (InterruptedException e) {
            // The server can be interrupted and stopped on a role switch.
            // It should not be treated as fatal
            log.warn("Server interrupted.  It could be due to a role switch");
        } catch (Throwable th) {
            log.error("LogReplicationServer exiting due to unrecoverable error:", th);
            System.exit(EXIT_ERROR_CODE);
        }
    }

    /**
     * Invoked on a role switch.  This method does not delete the netty event
     * loop groups passed in the server context.  It shuts down the server
     * router, LogReplicationServer and Sink Managers.
     *
     * Note: The server context is reused throughout the lifecycle of an LR
     * JVM.  So deleting the event loop groups makes them unusable on a
     * subsequent role switch to Sink.  The planned fix is to create the
     * groups in the NettyLogReplicationServerChannelAdapter when needed.
     * Once it is available, the below method can be removed and callers can
     * use the close() method which cleans up everything.
     * Eventually, passing the server context should also be eliminated
     * completely.
     *
     * Also note that the above limitation exists only if using the netty
     * transport adapter.  GRPC transport adapter does not result in any such
     * error.
     */
    public void disable() {
        log.trace("Disabling the Replication Server Node");
        cleanupResources();
    }

    /**
     * Closes the currently running corfu log replication server.
     */
    @Override
    public void close() {
        if (!close.compareAndSet(false, true)) {
            log.trace("close: Log Replication Server already shutdown");
            return;
        }
        log.info("close: Shutting down Log Replication server and cleaning resources");
        serverContext.close();
    }

    private void cleanupResources() {
        this.transportLayerServer.stop();

        // A executor service to create the shutdown threads
        // plus name the threads correctly.
        final ExecutorService shutdownService = Executors.newFixedThreadPool(serverMap.size(),
            new ServerThreadFactory("ReplicationCorfuServer-shutdown-",
                new ServerThreadFactory.ExceptionHandler()));

        // Turn into a list of futures on the shutdown, returning
        // generating a log message to inform of the result.
        CompletableFuture[] shutdownFutures = serverMap.values().stream()
            .map(server -> CompletableFuture.runAsync(() -> {
                try {
                    log.info("Shutting down {}", server.getClass().getSimpleName());
                    server.shutdown();
                    log.info("Cleanly shutdown {}", server.getClass().getSimpleName());
                } catch (Exception e) {
                    log.error("Failed to cleanly shutdown {}",
                        server.getClass().getSimpleName(), e);
                }
            }, shutdownService))
            .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(shutdownFutures).join();
        shutdownService.shutdown();

        serverStarted.set(false);
        // Stop listening on the server channel
        logReplicationServerRunner.shutdownNow();

        log.info("Log Replication Server shutdown and resources released");
    }

    public void updateTopologyConfigId(long configId) {
        logReplicationServer.updateTopologyConfigId(configId);
    }

    public void setLeadership(boolean isLeader) {
        logReplicationServer.setLeadership(isLeader);
    }
}
