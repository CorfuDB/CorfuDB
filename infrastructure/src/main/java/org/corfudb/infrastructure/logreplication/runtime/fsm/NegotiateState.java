package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class NegotiateState implements CommunicationState {

    private LogReplicationCommunicationFSM fsm;

    private Optional<String> leaderEndpoint;

    private volatile AtomicBoolean inProgress = new AtomicBoolean(false);

    private ExecutorService workers;

    private LogReplicationClientRouter router;

    public NegotiateState(LogReplicationCommunicationFSM fsm, ExecutorService workers, LogReplicationClientRouter router) {
        this.fsm = fsm;
        this.workers = workers;
        this.router = router;
    }

    @Override
    public CommunicationStateType getType() {
        return CommunicationStateType.NEGOTIATE;
    }

    @Override
    public CommunicationState processEvent(CommunicationEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case CONNECTION_DOWN:
                String endpointDown = event.getEndpoint();
                // Update list of valid connections.
                fsm.updateDisconnectedEndpoints(endpointDown);

                // If the leader is the node that become unavailable, verify new leader and attempt to reconnect.
                if (leaderEndpoint.equals(endpointDown)) {
                    leaderEndpoint = Optional.empty();
                    // Reconnect to endpoint (asynchronously)
                    fsm.reconnectAsync(endpointDown);
                    return fsm.getStates().get(CommunicationStateType.VERIFY_LEADER);
                } else {
                    // If non-leader node loses connectivity, reconnect async
                    fsm.reconnectAsync(endpointDown);
                    return this;
                }
            case CONNECTION_UP:
                // Some node got connected, update connected endpoints
                fsm.updateConnectedEndpoints(event.getEndpoint());
                return this;
            case NEGOTIATE_COMPLETE:
                return fsm.getStates().get(CommunicationStateType.REPLICATE);
            case NEGOTIATE_FAILED:
                return this;
            case LEADER_NOT_FOUND:
                return fsm.getStates().get(CommunicationStateType.VERIFY_LEADER);
            default: {
                log.warn("Unexpected communication event {} when in init state.", event.getType());
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(CommunicationState from) {
        workers.submit(this::negotiate);
        // Start Negotiation (check if ongoing negotiation is in progress)
        if(!inProgress.get()) {
            // Start Negotiation

            inProgress.set(true);


            // Negotiation completed
            inProgress.set(false);
            fsm.input(new CommunicationEvent(CommunicationEvent.CommunicationEventType.NEGOTIATE_COMPLETE));
        }
    }

    private void negotiate() {
        try {
            if(fsm.getLeader().isPresent()) {
                String remoteLeader = fsm.getLeader().get();
                CompletableFuture<LogReplicationNegotiationResponse> cf = router.sendMessageAndGetCompletable(
                        new CorfuMsg(CorfuMsgType.LOG_REPLICATION_NEGOTIATION_REQUEST).setEpoch(0), remoteLeader);
                LogReplicationNegotiationResponse response = cf.get(LogReplicationCommunicationFSM.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

                // Notify of negotiation result
                // router.onNegotiation(response);

                // Negotiation to leader node completed, unblock channel.
                router.connectionFuture.complete(null);
            } else {
                // No leader found at the time of negotiation
                fsm.input(new CommunicationEvent(CommunicationEvent.CommunicationEventType.LEADER_NOT_FOUND));
            }
        } catch (Exception e) {
            log.error("Exception during negotiation", e);
            // If the cause of exception is leadership loss.
            fsm.input(new CommunicationEvent(CommunicationEvent.CommunicationEventType.NEGOTIATE_FAILED));
        }
    }

    @Override
    public void onExit(CommunicationState to) {

    }

    @Override
    public void clear() {

    }

    /**
     * Set Leader Endpoint, determined during the transition from VERIFY_LEADER
     * to NEGOTIATE state.
     *
     * @param endpoint leader node on remote cluster
     */
    public void setLeaderEndpoint(String endpoint) {
        this.leaderEndpoint = Optional.of(endpoint);
    }
}
