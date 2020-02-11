package org.corfudb.logreplication.fsm;

import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;

/**
 * This class represents an exception in the case where an unexpected event
 * is processed for a given state, i.e., an invalid transition is detected.
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
