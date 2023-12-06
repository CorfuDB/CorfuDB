package org.corfudb.infrastructure.logreplication.runtime.fsm;

/**
 * This class indicates an invalid transition has been detected,
 * i.e., unexpected event has been processed when in a given state.
 *
 * @author annym
 */
public class IllegalRuntimeTransitionException extends Exception {

    /**
     * Constructor
     *
     * @param event     incoming lock event
     * @param stateType current state type
     */
    public IllegalRuntimeTransitionException(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType event, LogReplicationRuntimeStateType stateType) {
        super(String.format("Illegal transition for event %s while in state %s", event, stateType));
    }

}