package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.ExceptionMsg;

import java.util.List;
import java.util.concurrent.ExecutorService;
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
    public abstract HandlerMethods getHandlerMethods();

    /**
     * Get the message handler for this instance.
     *
     * @return A message handler.
     */
    public void process(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        //todo need to see if the server is shutdown
        getHandlerMethods().execute(msg, ctx, r);
    }

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
     * Handle a incoming Netty message.
     *
     * @param msg An incoming message.
     * @param ctx The channel handler context.
     * @param r   The router that took in the message.
     */
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (getState() == ServerState.SHUTDOWN) {
            log.warn("Server received {} but is already shutdown.", msg.getMsgType().toString());
            return;
        }

        if (!isServerReadyToHandleMsg(msg)) {
            r.sendResponse(ctx, msg, CorfuMsgType.NOT_READY.msg());
            return;
        }

        try {
            process(msg, ctx, r);
        } catch (Exception ex) {
            // Log the exception during handling
            log.error("process: Unhandled exception processing {} message",
                    msg.getMsgType(), ex);
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_SERVER_EXCEPTION.payloadMsg(new ExceptionMsg(ex)));
        }
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

    public abstract List<ExecutorService> getExecutors();

    /**
     * Shutdown the server.
     */
    public void shutdown() {
        setState(ServerState.SHUTDOWN);
        getExecutors().forEach(ExecutorService::shutdownNow);
    }

    /**
     * The server state.
     * Represents server in a particular state: READY, NOT_READY, SHUTDOWN.
     */
    public enum ServerState {
        READY, SHUTDOWN
    }
}
