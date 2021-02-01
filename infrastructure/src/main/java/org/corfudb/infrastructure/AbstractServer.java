package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;

import static org.corfudb.protocols.CorfuProtocolServerErrors.getNotReadyErrorMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;

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
     * @deprecated [RM]
     * Get the message handler for this instance.
     *
     * @return A message handler.
     */
    @Deprecated
    public HandlerMethods getHandler() {
        throw new UnsupportedOperationException("This operation is not supported");
    }

    /**
     * Get the request handlers for this instance.
     *
     * @return The request handler methods.
     */
    public abstract RequestHandlerMethods getHandlerMethods();

    /**
     * Seal the server with the epoch.
     *
     * @param epoch Epoch to seal with
     */
    public void sealServerWithEpoch(long epoch) {
        // Overridden in log unit to flush operations stamped with an old epoch
    }

    /**
     * @deprecated [RM]
     * @param msg  The incoming message.
     * @return  True if the server is ready to handle this message, and false otherwise.
     */
    @Deprecated
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        throw new UnsupportedOperationException("This operation is not supported");
    }

    /**
     * Determine if the server is ready to handle a request.
     * @param request The incoming request message.
     * @return True if the server is ready to handle this request, and false otherwise.
     */
    public boolean isServerReadyToHandleMsg(RequestMsg request) {
        return getState() == ServerState.READY;
    }

    /**
     * @deprecated [RM]
     * A stub that handlers can override to manage their threading, otherwise
     * the requests will be executed on the IO threads
     * @param msg
     * @param ctx
     * @param r
     */
    @Deprecated
    protected void processRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        throw new UnsupportedOperationException("This operation is not supported");
    }

    /**
     * A stub that handlers can override to manage their threading, otherwise
     * the requests will be executed on the IO threads
     * @param req An incoming request message.
     * @param ctx The channel handler context.
     * @param r The router that took in the request.
     */
    protected abstract void processRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r);


    /**
     * @deprecated [RM]
     * Handle a incoming Netty message.
     *
     * @param msg An incoming message.
     * @param ctx The channel handler context.
     * @param r   The router that took in the message.
     */
    @Deprecated
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
     * Handle a incoming request message.
     *
     * @param req An incoming request message.
     * @param ctx The channel handler context.
     * @param r   The router that took in the request message.
     */
    public final void handleMessage(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        if (getState() == ServerState.SHUTDOWN) {
            log.warn("handleMessage[{}]: Server received {} but is already shutdown.",
                    req.getHeader().getRequestId(), req.getPayload().getPayloadCase());
            return;
        }

        if (!isServerReadyToHandleMsg(req)) {
            r.sendResponse(getNotReadyError(req.getHeader()), ctx);
            return;
        }

        processRequest(req, ctx, r);
    }

    private ResponseMsg getNotReadyError(HeaderMsg requestHeader) {
        HeaderMsg responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        return getResponseMsg(responseHeader, getNotReadyErrorMsg());
    }

    /**
     * @deprecated [RM]
     * Handle an incoming message (not Netty specific).
     *
     * @param msg An incoming message.
     * @param r   The router that took in the message.
     */
    @Deprecated
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
