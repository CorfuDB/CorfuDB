package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.LogReplicationServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerThreadFactory;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationServerRouter;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class CorfuInterClusterReplicationServerNode implements AutoCloseable {

    @Getter
    private final ServerContext serverContext;

    @Getter
    private final Map<Class, AbstractServer> serverMap;

    @Getter
    private final LogReplicationServerRouter router;

    @Getter
    private LogReplicationConfig logReplicationConfig;

    // This flag makes the closing of the CorfuServer idempotent.
    private final AtomicBoolean close;

    /**
     * Corfu Server initialization.
     *
     * @param serverContext Initialized Server Context.
     * @param server log replication server handler
     * @param
     */
    public CorfuInterClusterReplicationServerNode(@Nonnull ServerContext serverContext,
                                                  @Nonnull LogReplicationServer server,
                                                  @Nonnull LogReplicationConfig config) {
        this(serverContext,
                ImmutableMap.<Class, AbstractServer>builder()
                        .put(BaseServer.class, new BaseServer(serverContext))
                        .put(LogReplicationServer.class, server)
                        .build()
        );
        this.logReplicationConfig = config;
    }

    /**
     * Corfu Server initialization.
     *
     * @param serverContext Initialized Server Context.
     * @param serverMap     Server Map with all components.
     */
    public CorfuInterClusterReplicationServerNode(@Nonnull ServerContext serverContext,
                                                  @Nonnull Map<Class, AbstractServer> serverMap) {
        this.serverContext = serverContext;
        this.serverMap = serverMap;

        this.close = new AtomicBoolean(false);
        this.router = new LogReplicationServerRouter(new ArrayList<>(serverMap.values()));
        this.serverContext.setServerRouter(router);
    }

    /**
     * Wait on Corfu Server Channel until it closes.
     */
    public void startAndListen() {
        try {
            log.info("Starting server transport adapter...");
            router.getServerAdapter().start().get();
        } catch (Exception e) {
            throw new UnrecoverableCorfuError(e);
        }
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

        this.router.getServerAdapter().stop();
        this.getLogReplicationServer().getSinkManager().shutdown();

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
        log.info("close: Log Replication Server shutdown and resources released");
    }

    public LogReplicationServer getLogReplicationServer() {
        return (LogReplicationServer)serverMap.get(LogReplicationServer.class);
    }
}
