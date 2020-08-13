package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;

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
     * @param msg
     * @param ctx
     * @param r
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

    /**
     * Handle an incoming message (not Netty specific).
     *
     * @param msg An incoming message.
     * @param r   The router that took in the message.
     */
    public final void handleMessage(CorfuMsg msg, IServerRouter r) {
        if (getState() == ServerState.SHUTDOWN) {
            log.warn("Server received {} but is already shutdown.", msg.getMsgType().toString());
            return;
        }

        if (!isServerReadyToHandleMsg(msg)) {
            r.sendResponse(msg, CorfuMsgType.NOT_READY.msg());
            return;
        }

        processRequest(msg, null, r);
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
    public void shutdown() {
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
