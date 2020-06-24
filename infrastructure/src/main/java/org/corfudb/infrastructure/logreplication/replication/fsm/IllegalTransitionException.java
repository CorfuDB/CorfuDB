package org.corfudb.infrastructure.logreplication.replication.fsm;

import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;

/**
 * This class indicates an invalid transition has been detected,
 * i.e., unexpected event has been processed when in a given state.
 */
public class IllegalTransitionException extends Exception {

    /**
     * Constructor
     *
     * @param eventType incoming log replication event type
     * @param stateType current state type
     */
    public IllegalTransitionException(LogReplicationEventType eventType, LogReplicationStateType stateType) {
        super(String.format("Illegal transition for event %s while in state %s", eventType, stateType));
    }
}
