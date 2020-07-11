package org.corfudb.utils.lock.states;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.utils.lock.Lock;

import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class represents the state when the <class>Lock</class> has does not have the lease. The lock has either never
 * acquired the lease or has lost it.
 * In this state, the lock waits for LEASE_REVOKED event to try and acquire the lease for the lock. It can only
 * acquire the lease if no other client holds a lease that has not expired.
 *
 * @author mdhawan
 * @since 04/17/2020
 */
@Slf4j
public class NoLeaseState extends LockState {

    public NoLeaseState(Lock lock) {
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
        log.debug("Lock: {} lock event:{} in state:{}", lock.getLockId(), event, getType());
        switch (event) {
            case LEASE_ACQUIRED: {
                // transition to has lease state
                return Optional.of(lock.getStates().get(LockStateType.HAS_LEASE));
            }
            case LEASE_RENEWED: {
                // can happen only if this event arrived late. Lease had already expired.
                log.warn("Lock: {} renewed event arrived late!", lock.getLockId());
                return Optional.empty();
            }
            case LEASE_REVOKED: {
                //lock client revoked the lease on this lock, should try to acquire lease.
                acquireLease();
                return Optional.empty();
            }
            case LEASE_EXPIRED: {
                log.warn("Lock: {} lock was expired by local monitor!", lock.getLockId());
                return Optional.empty();
            }
            case UNRECOVERABLE_ERROR: {
                log.error("Lock: {} unexpected lock event:{} in state:{}", lock.getLockId(), event, getType());
                return Optional.of(lock.getStates().get(LockStateType.STOPPED));
            }
            default:
                log.error("Unexpected lock event {} when in state {}.", event, getType());
                throw new IllegalTransitionException(event, getType());
        }
    }

    /**
     *
     * @param from LockState transitioning from.
     */
    @Override
    public void onEntry(LockState from) {}

    /**
     *
     *
     * @param to LockState transitioning to.
     */
    @Override
    public void onExit(LockState to) {}

    @Override
    public LockStateType getType() {
        return LockStateType.NO_LEASE;
    }

    /**** Helper Methods ***/

    /**
     * Tries to acquire the lease. If successful, it generates LEASE_ACQUIRED event.
     */
    private void acquireLease() {
        try {
            if (lockStore.acquire(lock.getLockId())) {
                lock.input(LockEvent.LEASE_ACQUIRED);
            }
        } catch (Exception e) {
            log.error("Lock: {} could not acquire lease {}", lock.getLockId(), e);
        }
    }

}
