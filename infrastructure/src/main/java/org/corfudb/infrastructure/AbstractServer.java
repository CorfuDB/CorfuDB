package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.util.CFUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by mwei on 12/4/15.
 */
@Slf4j
public abstract class AbstractServer {
    /**
     * Current server state
     */
    private final AtomicReference<ServerState> state = new AtomicReference<>(ServerState.READY);

    protected final ExecutorService shutdownService = Executors.newSingleThreadExecutor(
            ServerThreadFactory.create("corfu-shutdown-service-")
    );

    /**
     * Get the message handler for this instance.
     *
     * @return A message handler.
     */
    public abstract HandlerMethods getHandler();

    /**
     * Seal the server with the epoch.
     *
     * @param epoch Epoch to seal with
     */
    public void sealServerWithEpoch(long epoch) {
        // Overridden in log unit to flush operations stamped with an old epoch
    }

    public abstract boolean isServerReadyToHandleMsg(CorfuMsg msg);

    /**
     * A stub that handlers can override to manage their threading, otherwise
     * the requests will be executed on the IO threads
     * @param msg message
     * @param ctx netty channel context
     * @param r server router
     */
    protected void processRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        getHandler().handle(msg, ctx, r);
    }

    /**
     * Handle a incoming Netty message.
     *
     * @param msg An incoming message.
     * @param ctx The channel handler context.
     * @param r   The router that took in the message.
     */
    public final void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (getState() == ServerState.SHUTDOWN) {
            log.warn("Server received {} but is already shutdown.", msg.getMsgType().toString());
            return;
        }

        if (!isServerReadyToHandleMsg(msg)) {
            r.sendResponse(ctx, msg, CorfuMsgType.NOT_READY.msg());
            return;
        }

        processRequest(msg, ctx, r);
    }

    protected void setState(ServerState newState) {
        state.updateAndGet(currState -> {
            if (currState == ServerState.SHUTDOWN && newState != ServerState.SHUTDOWN) {
                throw new IllegalStateException("Server is in SHUTDOWN state. Can't be changed");
            }

            return newState;
        });
    }

    public ServerState getState() {
        return state.get();
    }

    /**
     * Shutdown the server.
     */
    public abstract CompletableFuture<Void> shutdown();

    protected CompletableFuture<Void> shutdownServerExecutor(ExecutorService executor) {
        String serverName = getClass().getSimpleName();
        log.info("close: Shutting down {}", serverName);

        return CFUtils
                .asyncShutdownExceptionally(executor, ServerContext.SHUTDOWN_TIMER, shutdownService);
    }

    protected void markShutdown(){
        setState(ServerState.SHUTDOWN);
    }

    /**
     * The server state.
     * Represents server in a particular state: READY, NOT_READY, SHUTDOWN.
     */
    public enum ServerState {
        READY, SHUTDOWN
    }
}
