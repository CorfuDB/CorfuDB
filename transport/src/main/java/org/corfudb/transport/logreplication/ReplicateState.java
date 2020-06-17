package org.corfudb.transport.logreplication;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReplicateState implements CommunicationState {

    private LogReplicationCommunicationFSM fsm;

    public ReplicateState(LogReplicationCommunicationFSM fsm) {
        this.fsm = fsm;
    }

    @Override
    public CommunicationStateType getType() {
        return CommunicationStateType.REPLICATE;
    }

    @Override
    public CommunicationState processEvent(CommunicationEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case CONNECTION_DOWN:
                String endpointDown = event.getEndpoint();
                // Update list of valid connections.
                fsm.updateDisconnectedEndpoints(endpointDown);
                // Reconnect to endpoint (asynchronously)
                fsm.reconnectAsync(endpointDown);

                // If the leader is the node that become unavailable, verify new leader and attempt to reconnect.
                if (fsm.getLeader().isPresent() && fsm.getLeader().get().equals(endpointDown)) {
                    // If remaining connections verify leadership on connected endpoints, otherwise, return to init
                    // state, until a connection is available.
                    return fsm.getConnectedEndpoints().size() == 0 ? fsm.getStates().get(CommunicationStateType.INIT) :
                            fsm.getStates().get(CommunicationStateType.VERIFY_LEADER);
                }

                // If a non-leader node loses connectivity, reconnect async and continue.
                return this;
            case CONNECTION_UP:
                // Some node got connected, update connected endpoints
                fsm.updateConnectedEndpoints(event.getEndpoint());
                return this;
            case NEGOTIATE_COMPLETE:
                return fsm.getStates().get(CommunicationStateType.REPLICATE);
            default: {
                log.warn("Unexpected communication event {} when in init state.", event.getType());
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(CommunicationState from) {

    }

    @Override
    public void onExit(CommunicationState to) {

    }

    @Override
    public void clear() {

    }

}
