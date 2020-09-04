package org.corfudb.utils.lock.states;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.utils.lock.Lock;

import java.util.Optional;

/**
 * Stopped State. Lock transitions to stopped state only if there is
 * an unrecoverable error.
 *
 * @author mdhawan
 * @since 04/17/2020
 */
@Slf4j
public class StoppedState extends LockState {

    public StoppedState(Lock lock) {
        super(lock);
    }

    /**
     * Processes events in this state.
     *
     * @param event
     * @return
     * @throws IllegalTransitionException
     */
    @Override
    public Optional<LockState> processEvent(LockEvent event) throws IllegalTransitionException {
        switch (event) {
            case LEASE_ACQUIRED:
            case LEASE_RENEWED:
            case LEASE_REVOKED:
            case LEASE_EXPIRED:
            case UNRECOVERABLE_ERROR:
                return Optional.empty();
            default:
                log.warn("Unexpected lock event {} when in state {}.", event, getType());
                throw new IllegalTransitionException(event, getType());
        }
    }


    @Override
    public void onEntry(LockState from) {
    }


    @Override
    public void onExit(LockState to) {
    }


    @Override
    public LockStateType getType() {
        return LockStateType.STOPPED;
    }
}
