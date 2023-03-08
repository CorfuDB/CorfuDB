package org.corfudb.infrastructure.logreplication.infrastructure.msgHandlers;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.logreplication.transport.IClientServerRouter;
import org.corfudb.protocols.service.CorfuProtocolMessage;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.util.concurrent.atomic.AtomicReference;

import static org.corfudb.protocols.CorfuProtocolServerErrors.getNotReadyErrorMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;

@Slf4j
public abstract class LogReplicationAbstractServer {

    /**
     * Current server state
     */
    private final AtomicReference<ReplicationServerState> state = new AtomicReference<>(ReplicationServerState.READY);

    /**
     * Get the message handlers for this instance.
     *
     * @return The request handler methods.
     */
    public abstract ReplicationHandlerMethods getHandlerMethods();

    /**
     * A stub that handlers can override to manage their threading, otherwise
     * the requests will be executed on the IO threads
     * @param req An incoming request message.
     * @param r The router that took in the request.
     */
    protected abstract void processRequest(CorfuMessage.RequestMsg req, CorfuMessage.ResponseMsg res, IClientServerRouter r);


    /**
     * Handle a incoming request message.
     *
     * @param req An incoming request message.
     * @param r   The router that took in the request message.
     */
    public final void handleMessage(CorfuMessage.RequestMsg req, CorfuMessage.ResponseMsg res, IClientServerRouter r) {
        if (getState() == ReplicationServerState.SHUTDOWN) {
            if (req == null) {
                log.warn("handleMessage[{}]: Server received {} but is already shutdown.",
                        res.getHeader().getRequestId(), res.getPayload().getPayloadCase());
            } else {
                log.warn("handleMessage[{}]: Server received {} but is already shutdown.",
                        req.getHeader().getRequestId(), req.getPayload().getPayloadCase());
            }
            return;
        }

        if (getState() != ReplicationServerState.READY) {
            if (req == null) {
                r.sendResponse(getNotReadyError(res.getHeader()));
            } else {
                r.sendResponse(getNotReadyError(req.getHeader()));
            }
            return;
        }

        processRequest(req, null, r);
    }

    private CorfuMessage.ResponseMsg getNotReadyError(CorfuMessage.HeaderMsg requestHeader) {
        //Shama check how this is used...CHECK and IGNORE
        CorfuMessage.HeaderMsg responseHeader = getHeaderMsg(requestHeader, CorfuProtocolMessage.ClusterIdCheck.CHECK, CorfuProtocolMessage.EpochCheck.IGNORE);
        return getResponseMsg(responseHeader, getNotReadyErrorMsg());
    }

    protected void setState(ReplicationServerState newState) {
        state.updateAndGet(currState -> {
            if (currState == ReplicationServerState.SHUTDOWN && newState != ReplicationServerState.SHUTDOWN) {
                throw new IllegalStateException("Server is in SHUTDOWN state. Can't be changed");
            }

            return newState;
        });
    }

    public ReplicationServerState getState() {
        return state.get();
    }

    /**
     * Shutdown the server.
     */
    public void shutdown() {
        setState(ReplicationServerState.SHUTDOWN);
    }

    /**
     * The server state.
     * Represents server in a particular state: READY, NOT_READY, SHUTDOWN.
     */
    public enum ReplicationServerState {
        READY, SHUTDOWN
    }

}
