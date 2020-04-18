package org.corfudb.utils.lock.states;

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

    // lease duration in 300 seconds
    public static int LEASE_DURATION = 300;

    // renew lease every 60 seconds
    public static int DURATION_BETWEEN_LEASE_RENEWALS = 60;

    // max time allowed for listener to process a notification
    public static int MAX_TIME_FOR_NOTIFICATION_LISTENER_PROCESSING = 60;


    // Lock being acquired
    protected final Lock lock;
    // Lock store to be used by the Lock states
    protected final LockStore lockStore;
    // Task scheduler to be used by lock states
    protected final ScheduledExecutorService taskScheduler;
    // Listener executor (for locklost, lockAcquired) notifications.
    protected final ExecutorService lockListenerExecutor;

    public LockState(Lock lock) {
        this.lock = lock;
        this.lockStore = lock.getClientContext().getLockStore();
        this.taskScheduler = lock.getClientContext().getTaskScheduler();
        this.lockListenerExecutor = lock.getClientContext().getLockListenerExecutor();
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



