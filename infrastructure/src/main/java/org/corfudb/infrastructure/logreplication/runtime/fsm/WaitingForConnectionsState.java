package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;

/**
 * Communication FSM Idle state (waiting for connections to come up)
 **
 * @author annym
 */
@Slf4j
public class WaitingForConnectionsState implements LogReplicationRuntimeState {

    private CorfuLogReplicationRuntime fsm;

    public WaitingForConnectionsState(CorfuLogReplicationRuntime fsm) {
        this.fsm = fsm;
    }

    @Override
    public LogReplicationRuntimeStateType getType() {
        return LogReplicationRuntimeStateType.WAITING_FOR_CONNECTIVITY;
    }

    @Override
    public LogReplicationRuntimeState processEvent(LogReplicationRuntimeEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case ON_CONNECTION_UP:
                log.info("On connection up, event={}", event);
                // Set Connected Endpoint for event transition.
                fsm.updateConnectedNodes(event.getNodeId());
                return fsm.getStates().get(LogReplicationRuntimeStateType.VERIFYING_REMOTE_LEADER);
            case LOCAL_LEADER_LOSS:
                return fsm.getStates().get(LogReplicationRuntimeStateType.STOPPED);
            default: {
                log.warn("Unexpected communication event {} when in init state.", event.getType());
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(LogReplicationRuntimeState from) {
        // Wait for connections to come up ..
        log.info("Waiting for connections to remote cluster to be established..");
    }
}
