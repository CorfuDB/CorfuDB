package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;

import javax.annotation.Nonnull;
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

    private ScheduledExecutorService logReplicationServerRunner;

    // Error code required to detect an ungraceful shutdown.
    private static final int EXIT_ERROR_CODE = 100;

    // this flag makes the start/stop operation of transportLayerServer idempotent
    @Getter
    private AtomicBoolean serverStarted;

    private final LogReplicationClientServerRouter router;

    /**
     * Log Replication Server initialization.
     *
     * @param serverContext Initialized Server Context
     * @param router Interface between LogReplication and the transport layer
     */
    public CorfuInterClusterReplicationServerNode(@Nonnull ServerContext serverContext,
                                                  LogReplicationClientServerRouter router) {

        this.serverContext = serverContext;
        this.router = router;
        this.serverStarted = new AtomicBoolean(false);
        setRouterAndStartServer();
    }

    public void setRouterAndStartServer() {
        if (!serverStarted.get()) {
            logReplicationServerRunner = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setNameFormat("replication-server-runner").build());
            // Start and listen to the server
            logReplicationServerRunner.submit(() -> this.startAndListen());
        } else {
            log.info("Server transport adapter is already running. Updating the router information");
        }
    }

    /**
     * Wait on Corfu Server Channel until it closes.
     */
    private void startAndListen() {
        try {
            log.info("Starting server transport adapter ...");
            serverStarted.set(true);
            this.router.createTransportServerAdapter(serverContext).get();
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
        if (!serverStarted.get()) {
            log.trace("close: Log Replication Server already shutdown");
            return;
        }
        log.trace("Disabling the Replication Server Node");
        cleanupResources();
    }

    /**
     * Closes the currently running corfu log replication server.
     */
    @Override
    public void close() {
        if (!serverStarted.get()) {
            log.trace("close: Log Replication Server already shutdown");
            return;
        }
        log.info("close: Shutting down Log Replication server and cleaning resources");
        cleanupResources();
    }

    private void cleanupResources() {
        this.router.getServerChannelAdapter().stop();
        serverStarted.set(false);
        // Stop listening on the server channel
        logReplicationServerRunner.shutdownNow();

        log.info("Log Replication Server shutdown and resources released");
    }

}
