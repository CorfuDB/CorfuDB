package org.corfudb.utils.lock.states;

/**
 * This class indicates an invalid transition has been detected,
 * i.e., unexpected event has been processed when in a given state.
 *
 * @author mdhawan
 * @since 04/17/2020
 */
public class IllegalTransitionException extends Exception {

    /**
     * Constructor
     *
     * @param event     incoming lock event
     * @param stateType current state type
     */
    public IllegalTransitionException(LockEvent event, LockStateType stateType) {
        super(String.format("Illegal transition for event %s while in state %s", event, stateType));
    }

}
