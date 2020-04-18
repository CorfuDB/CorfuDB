package org.corfudb.utils.lock.states;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.utils.lock.Lock;

import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class represents the state when the <class>Lock</class> has does not have the lease. The lock has either never
 * acquired the lease or has lost it. In this state the Lock periodically attempts to acquire the lease . If no
 * client holds an unexpired lease to the lock, this lock will acquire it in it's next acquire attempt.
 *
 * @author mdhawan
 * @since 04/17/2020
 */
@Slf4j
public class NoLeaseState extends LockState {

    // try to acquire lease every 60 seconds
    private static int DURATION_BETWEEN_LEASE_ACQUISTION_ATTEMPTS = 60;

    private Optional<ScheduledFuture<?>> leaseAcquisitionFuture = Optional.empty();


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
        log.debug("Lock: {} Client:{} lock event:{} in state:{}", lock.getLockId(), lock.getClientUuid(), event, getType());
        switch (event) {
            case START_LOCK_FSM: {
                // first event to start the fsm
                // try to acquire the lease.
                startLeaseAcquisition(0);
            }
            case LEASE_ACQUIRED: {
                // transition to has lease state
                return Optional.of(lock.getStates().get(LockStateType.HAS_LEASE));
            }
            case LEASE_RENEWED: {
                // can happen only if this event arrived late. Lease had already expired.
                log.warn("Lock: {} Client:{} renewed event arrived late!", lock.getLockId(), lock.getClientUuid());
                return Optional.empty();
            }
            case LEASE_REVOKED: {
                log.warn("Lock: {} Client:{} lock was expired by another client!", lock.getLockId(), lock.getClientUuid());
                return Optional.empty();
            }
            case LEASE_EXPIRED: {
                log.warn("Lock: {} Client:{} lock was expired by local monitor!", lock.getLockId(), lock.getClientUuid());
                return Optional.empty();
            }
            case UNRECOVERABLE_ERROR: {
                log.error("Lock: {} Client:{} unexpected lock event:{} in state:{}", lock.getLockId(), lock.getClientUuid(), event, getType());
                return Optional.of(lock.getStates().get(LockStateType.STOPPED));
            }
            default:
                log.error("Unexpected lock event {} when in state {}.", event, getType());
                throw new IllegalTransitionException(event, getType());
        }
    }

    /**
     * Starts periodic lease acquisition task on entry to this state.
     * Delay the lease acquisition task on this node as it just lost
     * lock. Give other instances a higher chance to acquire lock.
     *
     * @param from LockState transitioning from.
     */
    @Override
    public void onEntry(LockState from) {
        startLeaseAcquisition(LEASE_DURATION);
    }

    /**
     * Stops lease acquire task.
     *
     * @param to LockState transitioning to.
     */
    @Override
    public void onExit(LockState to) {
        stopLeaseAcquisition();
    }

    @Override
    public LockStateType getType() {
        return LockStateType.NO_LEASE;
    }

    /**** Helper Methods ***/


    /**
     * Start lease acquisition task.
     */
    private void startLeaseAcquisition(int initialDelay) {
        synchronized (leaseAcquisitionFuture) {
            if (!leaseAcquisitionFuture.isPresent() || leaseAcquisitionFuture.get().isDone())
                leaseAcquisitionFuture = Optional.of(taskScheduler.scheduleWithFixedDelay(
                        () -> {
                            try {
                                if (lockStore.acquire(lock.getLockId(), lock.getClientUuid())) {
                                    lock.input(LockEvent.LEASE_ACQUIRED);
                                }
                            } catch (Exception e) {
                                log.error("Lock: {} Client:{} could not acquire lock {}", lock.getLockId(), lock.getClientUuid(), e);
                            }
                        },
                        initialDelay,
                        DURATION_BETWEEN_LEASE_ACQUISTION_ATTEMPTS,
                        TimeUnit.SECONDS
                ));
        }

    }


    /**
     * Stop lease acquisition task.
     */
    private void stopLeaseAcquisition() {
        synchronized (leaseAcquisitionFuture) {
            if (leaseAcquisitionFuture.isPresent() && !leaseAcquisitionFuture.get().isDone()) {
                boolean cancelled = leaseAcquisitionFuture.get().cancel(false);
                if (!cancelled) {
                    log.error("Lock: {} Client:{} Could not cancel the future for lease acquisition.", lock.getLockId(), lock.getClientUuid());
                }
            }
        }
    }


}
