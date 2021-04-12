/*
 ***********************************************************************
 * Copyright 2019 VMware, Inc.  All rights reserved. VMware Confidential
 ***********************************************************************
 */

package org.corfudb.utils.lock;


import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.utils.lock.LockDataTypes.LockId;
import org.corfudb.utils.lock.persistence.LockStore;
import org.corfudb.utils.lock.states.LockEvent;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Applications can register interest for a lock using the LockClient. When a lock is acquired on behalf of an instance
 * of an application it is notified through registered callbacks. Similarly if a lock is lost/revoked the corresponding
 * application instance is notified through callbacks.
 * <p>
 * The client also monitors the registered locks. If a lock has an expired lease, it generates a LEASE_REVOKED
 * event on that lock.
 *
 * @author mdhawan
 * @since 04/17/2020
 */
// TODO Figure out if the lock client should be a singleton
@Slf4j
public class LockClient {

    // all the locks that the applications are interested in.
    @VisibleForTesting
    @Getter
    private final Map<LockId, Lock> locks = new ConcurrentHashMap<>();

    // Lock data store
    private final LockStore lockStore;

    // Single threaded scheduler to monitor locks
    private final ScheduledExecutorService lockMonitorScheduler;

    private final ScheduledExecutorService taskScheduler;

    private final ExecutorService lockListenerExecutor;

    // duration between monitoring runs
    @Setter
    private static int DurationBetweenLockMonitorRuns = 10;

    // The context contains objects that are shared across the locks in this client.
    private final ClientContext clientContext;

    @Getter
    private UUID clientId;

    /**
     * Constructor
     *
     * @param clientId
     * @param corfuRuntime
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    //TODO need to determine if the application should provide a clientId or should it be internally generated.
    public LockClient(UUID clientId, CorfuRuntime corfuRuntime) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        this.taskScheduler = Executors.newScheduledThreadPool(1, (r) ->
        {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("LockTaskThread");
            t.setDaemon(true);
            return t;
        });

        // Single threaded scheduler to monitor the acquired locks (lease)
        // A dedicated scheduler is required in case the task scheduler is stuck in some database operation
        // and the previous lock owner can effectively expire the lock.
        ScheduledExecutorService leaseMonitorScheduler = Executors.newScheduledThreadPool(1, (r) ->
        {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("LeaseMonitorThread");
            t.setDaemon(true);
            return t;
        });

        this.lockListenerExecutor = Executors.newFixedThreadPool(1, (r) ->
        {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("LockListenerThread");
            t.setDaemon(true);
            return t;
        });

        this.lockMonitorScheduler = Executors.newScheduledThreadPool(1, (r) ->
        {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("LockMonitorThread");
            t.setDaemon(true);
            return t;
        });

        this.clientId = clientId;
        this.lockStore = new LockStore(corfuRuntime, clientId);
        this.clientContext = new ClientContext(clientId, lockStore, taskScheduler, lockListenerExecutor, leaseMonitorScheduler);
    }

    /**
     * Application registers interest for a lock [lockgroup, lockname]. The <class>Lock</class> will then
     * make periodic attempts to acquire lock. Lock is acquired when the <class>Lock</class> is able to write
     * a lease record in a common table that is being written to/read by all the registered <class>Lock</class>
     * instances. Once acquired, the lease for the lock needs to be renewed periodically or else it will be acquired by
     * another contending <class>Lock</class> instance. The application is notified if a lock is lost.
     *
     * @param lockGroup
     * @param lockName
     * @param lockListener
     */
    public void registerInterest(@NonNull String lockGroup, @NonNull String lockName, LockListener lockListener) {
        LockId lockId = LockDataTypes.LockId.newBuilder()
                .setLockGroup(lockGroup)
                .setLockName(lockName)
                .build();

        Lock lock = locks.computeIfAbsent(
                lockId,
                (key) -> new Lock(lockId, lockListener, clientContext));

        // Initialize the lease
        lock.input(LockEvent.LEASE_REVOKED);

        monitorLocks();
    }

    /**
     * Monitor all the locks this client is interested in.
     * If a lock has an expired lease, the lock will be revoked.
     **/
    private void monitorLocks() {
        // find the expired leases.
        // Handle for the periodic lock monitoring task
        lockMonitorScheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        Collection<LockId> locksWithExpiredLeases = lockStore.filterLocksWithExpiredLeases(locks.keySet());
                        for (LockId lockId : locksWithExpiredLeases) {
                            log.debug("LockClient: lease revoked for lock {}", lockId.getLockName());
                            locks.get(lockId).input(LockEvent.LEASE_REVOKED);
                        }
                    } catch (Exception ex) {
                        log.error("Caught exception while monitoring locks.", ex);
                    }
                },
                DurationBetweenLockMonitorRuns,
                DurationBetweenLockMonitorRuns,
                TimeUnit.SECONDS

        );
    }

    public void shutdown() {
        log.info("Shutdown Lock Client");
        this.lockMonitorScheduler.shutdown();
        this.taskScheduler.shutdown();
        this.lockListenerExecutor.shutdown();
    }

    /**
     * Context is used to provide access to common values and resources needed by objects implementing
     * the Lock functionality.
     */
    @Data
    public class ClientContext {

        private final UUID clientUuid;
        private final LockStore lockStore;
        private final ScheduledExecutorService taskScheduler;
        private final ScheduledExecutorService leaseMonitorScheduler;
        private final ExecutorService lockListenerExecutor;

        public ClientContext(UUID clientUuid, LockStore lockStore, ScheduledExecutorService taskScheduler,
                             ExecutorService lockListenerExecutor, ScheduledExecutorService leaseMonitorScheduler) {
            this.clientUuid = clientUuid;
            this.lockStore = lockStore;
            this.taskScheduler = taskScheduler;
            this.lockListenerExecutor = lockListenerExecutor;
            this.leaseMonitorScheduler = leaseMonitorScheduler;
        }
    }


}
