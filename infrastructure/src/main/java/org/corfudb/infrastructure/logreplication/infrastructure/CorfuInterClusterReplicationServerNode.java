package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerThreadFactory;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationServerRouter;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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


    private LogReplicationServer logReplicationServer;

    private ScheduledExecutorService logReplicationServerRunner;

    // Error code required to detect an ungraceful shutdown.
    private static final int EXIT_ERROR_CODE = 100;

    /**
     * Log Replication Server initialization.
     *
     * @param serverContext Initialized Server Context
     * @param logReplicationServer Replication Server which processes incoming requests
     */
    // TODO v2: the serverContext will need to evaluated to use corfu's construct vs create a new context for LR
    public CorfuInterClusterReplicationServerNode(@Nonnull ServerContext serverContext,
        LogReplicationServer logReplicationServer) {

        this.serverContext = serverContext;

        this.logReplicationServer = logReplicationServer;

        this.serverMap = ImmutableMap.<Class, AbstractServer>builder()
            .put(BaseServer.class, new BaseServer(serverContext))
            .put(LogReplicationServer.class, logReplicationServer)
            .build();

        this.close = new AtomicBoolean(false);
        this.router = new LogReplicationServerRouter(new ArrayList<>(serverMap.values()));
        this.serverContext.setServerRouter(router);

        logReplicationServerRunner = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setNameFormat("replication-server-runner").build());

        // Start and listen to the server
        logReplicationServerRunner.submit(this::startAndListen);
    }

    /**
     * Wait on the transport framework's server until it is shutdown.
     */
    private void startAndListen() {
        try {
            log.info("Starting server transport adapter...");
            router.getServerAdapter().start().get();
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
    // TODO V2: We had a disable() for netty. Bring it back along with serverContext for LR when netty is bought back
    public void close() {
        if (!close.compareAndSet(false, true)) {
            log.trace("close: Log Replication Server already shutdown");
            return;
        }
        log.info("close: Shutting down Log Replication server and cleaning resources");
        serverContext.close();
        cleanupResources();
    }

    private void cleanupResources() {
        this.router.getServerAdapter().stop();

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
