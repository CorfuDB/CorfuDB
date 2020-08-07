package org.corfudb.utils.lock.states;

import lombok.Setter;
import org.corfudb.utils.lock.Lock;
import org.corfudb.utils.lock.persistence.LockStore;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * All lock states extend from this abstract class.
 *
 * @author mdhawan
 * @since 04/17/2020
 */
public abstract class LockState {

    // Renew lease every 20 seconds
    @Setter
    public static int DurationBetweenLeaseRenewals = 20;

    // Max time allowed for listener to process a notification
    @Setter
    public static int MaxTimeForNotificationListenerProcessing = 60;

    // Lock being acquired
    protected final Lock lock;
    // Lock store to be used by the Lock states
    protected final LockStore lockStore;
    // Task scheduler to be used by lock states
    protected final ScheduledExecutorService taskScheduler;
    // Dedicated scheduler to be used in HasLeaseState to monitor acquired lock
    protected final ScheduledExecutorService leaseMonitorScheduler;
    // Listener executor (for lock lost, lockAcquired) notifications.
    protected final ExecutorService lockListenerExecutor;

    public LockState(Lock lock) {
        this.lock = lock;
        this.lockStore = lock.getClientContext().getLockStore();
        this.taskScheduler =  lock.getClientContext().getTaskScheduler();
        this.lockListenerExecutor =  lock.getClientContext().getLockListenerExecutor();
        this.leaseMonitorScheduler = lock.getClientContext().getLeaseMonitorScheduler();
    }

    /**
     * Get LockState type.
     */
    public abstract LockStateType getType();


    /**
     * Method to process an event related to Lock.
     *
     * @return next LockState to transition to.
     */
    public abstract Optional<LockState> processEvent(LockEvent event) throws IllegalTransitionException;

    /**
     * On Entry
     *
     * @param from LockState transitioning from.
     */
    public void onEntry(LockState from) {
    }

    /**
     * On Exit
     *
     * @param to LockState transitioning to.
     */
    public void onExit(LockState to) {
    }

    /**
     * Provides capability to clear/clean state information onEntry.
     */
    public void clear() {
    }
}



