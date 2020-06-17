package org.corfudb.transport.logreplication;

import lombok.extern.slf4j.Slf4j;

/**
 * Communication FSM Idle state (waiting for connections to come up)
 **
 * @author annym
 */
@Slf4j
public class IdleState implements CommunicationState {

    private LogReplicationCommunicationFSM fsm;


    public IdleState(LogReplicationCommunicationFSM fsm) {
        this.fsm = fsm;
    }

    @Override
    public CommunicationStateType getType() {
        return CommunicationStateType.INIT;
    }

    @Override
    public CommunicationState processEvent(CommunicationEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case CONNECTION_UP:
                // Set Connected Endpoint for event transition.
                fsm.updateConnectedEndpoints(event.getEndpoint());
                return fsm.getStates().get(CommunicationStateType.VERIFY_LEADER);
            default: {
                log.warn("Unexpected communication event {} when in init state.", event.getType());
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(CommunicationState from) {
        // Wait for connections to come up ..
    }
}
