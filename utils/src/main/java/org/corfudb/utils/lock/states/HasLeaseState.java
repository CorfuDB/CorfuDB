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

    private static int DURATION_BETWEEN_LEASE_MONITOR_CHECKS = 60;






    // task to renew lease
    private Optional<ScheduledFuture<?>> leaseRenewalFuture = Optional.empty();

    // task to monitor lease expiration
    private Optional<ScheduledFuture<?>> leaseMonitorFuture = Optional.empty();

    // records the last time lease was acquired or renewed.
    private Optional<Instant> leaseTime = Optional.empty();

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
        log.debug("Lock: {} Client:{} lock event:{} in state:{}", lock.getLockId(), lock.getClientUuid(), event, getType());
        switch (event) {
            case LEASE_ACQUIRED: {
                // This should not happen !
                // TODO maybe this is illegalStateTransition
                log.warn("Lock: {} Client:{} unexpected lock event:{} in state:{}", lock.getLockId(), lock.getClientUuid(), event, getType());
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
                log.error("Lock: {} Client:{} unexpected lock event:{} in state:{}", lock.getLockId(), lock.getClientUuid(), event, getType());
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
        notify(() -> lock.getLockListener().lockAcquired(), "Lock Acquired");
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
        notify(() -> lock.getLockListener().lockRevoked(), "Lock Revoked");
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
                log.error("Lock: {} Client:{} {} callback cancelled due to timeout!", lock.getLockId(), lock.getClientUuid(), callback);
            }
        }, MAX_TIME_FOR_NOTIFICATION_LISTENER_PROCESSING, TimeUnit.SECONDS);
    }

    /**
     * Starts lease renewal task.
     */
    private void startLeaseRenewal() {
        synchronized (leaseRenewalFuture) {
            if (!leaseRenewalFuture.isPresent() || leaseRenewalFuture.get().isDone())
                leaseRenewalFuture = Optional.of(taskScheduler.scheduleWithFixedDelay(
                        () -> {
                            try {
                                if (lockStore.renew(lock.getLockId(), lock.getClientUuid())) {
                                    lock.input(LockEvent.LEASE_RENEWED);
                                } else {
                                    lock.input(LockEvent.LEASE_REVOKED);
                                }
                            } catch (Exception e) {
                                log.error("Lock: {} Client:{} could not renew lease for lock {}", lock.getLockId(), lock.getClientUuid(), e);
                            }
                        },
                        0,
                        DURATION_BETWEEN_LEASE_RENEWALS,
                        TimeUnit.SECONDS
                ));
        }

    }

    /**
     * Starts lease Monitor task
     */
    private void startLeaseMonitor() {
        synchronized (leaseMonitorFuture) {
            if (!leaseMonitorFuture.isPresent() || leaseMonitorFuture.get().isDone())
                leaseMonitorFuture = Optional.of(taskScheduler.scheduleWithFixedDelay(
                        () -> {
                            if (leaseTime.isPresent() && leaseTime.get().isBefore(Instant.now().minusSeconds(LEASE_DURATION))) {
                                lock.input(LockEvent.LEASE_EXPIRED);
                            }

                        },
                        0,
                        DURATION_BETWEEN_LEASE_MONITOR_CHECKS,
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
                    log.error("Lock: {} Client:{} Could not cancel the future for lease renewal!!!", lock.getLockId(), lock.getClientUuid());
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
                    log.error("Lock: {} Client:{} Could not cancel the future for lease monitor!!!", lock.getLockId(), lock.getClientUuid());
                }
            }
        }
    }

}
