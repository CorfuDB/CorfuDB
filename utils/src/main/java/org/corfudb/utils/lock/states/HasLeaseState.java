package org.corfudb.utils.lock.states;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.utils.lock.Lock;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


/**
 * This class represents the state when the <class>Lock</class> has successfully acquired the lease.
 * In this state the Lock periodically attempts to renew the lease . If it is not able to renew the lease
 * before it expires, the lock loses the lease.
 *
 * @author mdhawan
 * @since 04/17/2020
 */
@Slf4j
public class HasLeaseState extends LockState {
    //TODO move these to configs.

    // duration after which a local task checks if a lease has expired.
    private static int DURATION_BETWEEN_LEASE_CHECKS = 60;


    // task to renew lease
    private Optional<ScheduledFuture<?>> leaseRenewalFuture = Optional.empty();

    // task to check if the lease has expired
    private Optional<ScheduledFuture<?>> leaseMonitorFuture = Optional.empty();

    // records the last time lease was acquired or renewed.
    // this variable is used to check whether the lease has expired.
    private volatile Optional<Instant> leaseTime = Optional.empty();

    /**
     * Constructor
     *
     * @param lock log replication state machine
     */
    public HasLeaseState(Lock lock) {
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
                // This should not happen !
                // TODO maybe this is illegalStateTransition
                log.warn("Lock: {} unexpected lock event:{} in state:{}", lock.getLockId(), event, getType());
                leaseTime = Optional.of(Instant.now());
                return Optional.empty();
            }
            case LEASE_RENEWED: {
                // Lock is expected to keep renewing it's lease
                // Record the new lease time
                leaseTime = Optional.of(Instant.now());
                return Optional.empty();
            }
            case LEASE_REVOKED:
                // When another client invoked the lease
            case LEASE_EXPIRED: {
                // When the local timer on the lease renewal expired.
                leaseTime = Optional.empty();
                return Optional.of(lock.getStates().get(LockStateType.NO_LEASE));
            }
            case UNRECOVERABLE_ERROR: {
                log.error("Lock: {} unexpected lock event:{} in state:{}", lock.getLockId(), event, getType());
                leaseTime = Optional.empty();
                return Optional.of(lock.getStates().get(LockStateType.STOPPED));
            }
            default: {
                log.error("Unexpected lock event {} when in state {}.", event, getType());
                throw new IllegalTransitionException(event, getType());
            }
        }
    }

    /**
     * The following steps are performed on acquiring a lock:
     * <ul>
     * <li>Application is notified that lock has been acquired</li>
     * <li>Lease time is recorded</li>
     * <li>Lease renewal task is started</li>
     * <li>Lease monitor task is started. Locally checks if a lease has expired . Expires the lease in case the lease
     * renewal task is stuck.</li>
     * </ul>
     *
     * @param from LockState transitioning from.
     */
    @Override
    public void onEntry(LockState from) {
        notify(() -> lock.getLockListener().lockAcquired(lock.getLockId()), "Lock Acquired");
        //Record the lease time
        leaseTime = Optional.of(Instant.now());
        startLeaseRenewal();
        startLeaseMonitor();
    }


    /**
     * The following steps are performed on losing lock:
     * <ul>
     * <li>Application is notified that lock is lost</li>
     * <li>Lease time is cleared.</li>
     * <li>Lease renewal task is stopped</li>
     * <li>Lease monitor task is stopped</li>
     * </ul>
     *
     * @param to LockState transitioning to.
     */
    @Override
    public void onExit(LockState to) {
        notify(() -> lock.getLockListener().lockRevoked(lock.getLockId()), "Lock Revoked");
        //Clear leaseTime.
        leaseTime = Optional.empty();
        stopLeaseRenewal();
        stopLeaseMonitor();
    }

    @Override
    public LockStateType getType() {
        return LockStateType.HAS_LEASE;
    }

    /**** Helper Methods ***/
    /**
     * Notifies the client of lockAcquired or lockLost.
     * It is executed on a LockListenerExecutor. A monitor task is also started
     * to make sure this task does not get stuck or hung.
     *
     * @param runnable
     * @param callback name of the callback [This is used to create error message]
     */
    private void notify(Runnable runnable, String callback) {
        Future<?> taskFuture = lockListenerExecutor.submit(runnable);
        taskScheduler.schedule(() -> {
            if (!taskFuture.isDone()) {
                taskFuture.cancel(true);
                log.error("Lock: {} {} callback cancelled due to timeout!", lock.getLockId(), callback);
            }
        }, MaxTimeForNotificationListenerProcessing, TimeUnit.SECONDS);
    }

    /**
     * Starts lease renewal task. Tries to renew the lease periodically so that it
     * does not expire. If lease expires, another client can acquire the lock.
     *
     */
    private void startLeaseRenewal() {
        // TODO: add hook / predicate to be run before lease renewal
        synchronized (leaseRenewalFuture) {
            if (!leaseRenewalFuture.isPresent() || leaseRenewalFuture.get().isDone())
                leaseRenewalFuture = Optional.of(taskScheduler.scheduleWithFixedDelay(
                        () -> {
                            try {
                                if (lockStore.renew(lock.getLockId())) {
                                    lock.input(LockEvent.LEASE_RENEWED);
                                    leaseTime = Optional.of(Instant.now());
                                } else {
                                    log.info("Lock: {} lease revoked for lock {}", lock.getLockId());
                                    lock.input(LockEvent.LEASE_REVOKED);
                                }
                            } catch (Exception e) {
                                log.error("Lock: {} could not renew lease for lock {}", lock.getLockId(), e);
                            }
                        },
                        0,
                        DurationBetweenLeaseRenewals,
                        TimeUnit.SECONDS
                ));
        }

    }

    /**
     * An independent task is run to make sure the lease is renewed on time. This task generates
     * a LEASE_EXPIRED event if the lease was not be renewed on time.
     */
    private void startLeaseMonitor() {
        synchronized (leaseMonitorFuture) {
            if (!leaseMonitorFuture.isPresent() || leaseMonitorFuture.get().isDone())
                leaseMonitorFuture = Optional.of(taskScheduler.scheduleWithFixedDelay(
                        () -> {
                            if (leaseTime.isPresent() && leaseTime.get().isBefore(Instant.now().minusSeconds(Lock.leaseDuration))) {
                                log.info("Lock: {} lease expired for lock {}", lock.getLockId());
                                lock.input(LockEvent.LEASE_EXPIRED);
                            }

                        },
                        0,
                        DURATION_BETWEEN_LEASE_CHECKS,
                        TimeUnit.SECONDS
                ));
        }

    }

    /**
     * Stops lease renewal task.
     */
    private void stopLeaseRenewal() {
        synchronized (leaseRenewalFuture) {
            if (leaseRenewalFuture.isPresent() && !leaseRenewalFuture.get().isDone()) {
                boolean cancelled = leaseRenewalFuture.get().cancel(false);
                if (!cancelled) {
                    log.error("Lock: Could not cancel the future for lease renewal!!!", lock.getLockId());
                }
            }
        }
    }

    /**
     * Stops lease monitor task.
     */
    private void stopLeaseMonitor() {
        synchronized (leaseMonitorFuture) {
            if (leaseMonitorFuture.isPresent() && !leaseMonitorFuture.get().isDone()) {
                boolean cancelled = leaseMonitorFuture.get().cancel(false);
                if (!cancelled) {
                    log.error("Lock: {} Could not cancel the future for lease monitor!!!", lock.getLockId());
                }
            }
        }
    }

}
