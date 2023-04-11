package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class manages the lifecycle of the transport layer server.
 * Used by connection receiving clusters.
 */
@Slf4j
public class CorfuInterClusterReplicationServerNode {

    @Getter
    private final ServerContext serverContext;

    // An executor service that enqueues start/stop transport server events
    private ScheduledExecutorService logReplicationServerRunner;

    // Error code required to detect an ungraceful shutdown.
    private static final int EXIT_ERROR_CODE = 100;

    // this flag makes the start/stop operation of transport layer server idempotent
    private boolean serverStarted;

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
        this.serverStarted = false;
        startServer();
    }

    public synchronized void startServer() {
        if (logReplicationServerRunner == null || logReplicationServerRunner.isShutdown()) {
            logReplicationServerRunner = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setNameFormat("replication-server-runner").build());
        }
        // Start and listen to the server
        logReplicationServerRunner.submit(() -> this.startAndListen());
    }

    /**
     * Wait on the transport frameworks's server until it is shutdown.
     */
    private void startAndListen() {
        if (!serverStarted) {
            log.info("Starting server transport adapter ...");
            CompletableFuture<Boolean> cf = this.router.createTransportServerAdapter(serverContext);
            try {
                serverStarted = cf.get();
            } catch (ExecutionException th) {
                log.error("LogReplicationServer exiting due to unrecoverable error: {}", th.getMessage());
                System.exit(EXIT_ERROR_CODE);
            } catch (InterruptedException e) {
                // The server can be interrupted and stopped on a role switch or on leadership loss.
                // It should not be treated as fatal
                log.warn("Server interrupted.  It could be due to a role switch");
            }
        } else {
            log.info("Server transport adapter is already running.");
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
    // TODO: comply with the comment when netty is bought back
    public void disable() {
        log.trace("Disabling the Replication Server Node");
        cleanupResources();
    }

    /**
     * Closes the currently running corfu log replication server.
     */
    public void close() {
        log.info("close: Shutting down Log Replication Inter Cluster Server and cleaning resources");
        cleanupResources();
    }

    private synchronized void cleanupResources() {
        logReplicationServerRunner.submit(() -> {
            if (serverStarted) {
                this.router.getServerChannelAdapter().stop();
                serverStarted = false;
            } else {
                log.trace("close: Log Replication Inter Cluster Server already shutdown");
                return;
            }
        });

        // Stop listening on the server channel
        logReplicationServerRunner.shutdown();

        // wait until logReplicationServerRunner finishes the submitted tasks and is shutdown
        try {
            logReplicationServerRunner.awaitTermination(ServerContext.SHUTDOWN_TIMER.toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch(InterruptedException ie) {
            log.info("Executor service was interrupted while trying to shutdown ", ie);
        }

        log.info("Log Replication Inter Cluster Server shutdown and resources released");
    }

}
