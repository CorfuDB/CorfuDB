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
    // TODO v2: the serverContext will need to evaluated to use corfu's construct vs create a new context for LR
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
     * Closes the currently running corfu log replication server.
     */
    // TODO V2: We had a disable() for netty. Bring it back along with serverContext for LR when netty is bought back
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
