package org.corfudb.infrastructure.server;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.server.CorfuServerStateGraph.CorfuServerState;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Builder
public class CorfuServerStateMachine {
    private final ExecutorService processor = Executors.newSingleThreadExecutor();

    private final CompletableFuture<Void> shutdownListener = new CompletableFuture<>();

    @NonNull
    private CompletableFuture<CorfuServerManager> actions;

    public synchronized CompletableFuture<CorfuServerState> next(CorfuServerState nextState) {
        actions = actions.thenApplyAsync(currentServer -> {
            switch (nextState) {
                case INIT:
                    log.warn("Invalid state: INIT");
                    return currentServer;
                case START:
                    return currentServer.startServer(this);
                case RESET_LOG_UNIT:
                    return currentServer.resetLogUnitServer();
                case STOP:
                    return currentServer.stopServer();
                case STOP_AND_CLEAN:
                    return currentServer.stopAndCleanServer();
                default:
                    throw new IllegalStateException("Invalid state: " + nextState);
            }
        }, processor);

        return actions.thenApply(CorfuServerManager::getCurrentState);
    }

    public void waitForShutdown() {
        shutdownListener.join();
    }

    public void registerShutdownHook(){
        Thread shutdownThread = new Thread(()-> shutdownListener.complete(null));
        shutdownThread.setName("app-shutdown-thread");
        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }
}
