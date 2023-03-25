package org.corfudb.infrastructure.logreplication.infrastructure.msgHandlers;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.transport.IClientServerRouter;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.util.concurrent.atomic.AtomicReference;

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
     * A stub that handlers can override to manage their threading, otherwise the messages will be executed on the IO
     * threads
     *
     * @param req An incoming request message.
     * @param res An incoming response message.
     * @param r The router that took in the request.
     */
    protected abstract void processRequest(CorfuMessage.RequestMsg req, CorfuMessage.ResponseMsg res, IClientServerRouter r);


    /**
     * Handle a incoming replication message.
     *
     * @param req An incoming request message.
     * @param res An incoming response message.
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

        processRequest(req, res, r);
    }

    /**
     * Set the state of the server
     *
     * @param newState
     */
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
     * Represents server in a particular state: READY, SHUTDOWN.
     */
    public enum ReplicationServerState {
        READY, SHUTDOWN
    }

}
