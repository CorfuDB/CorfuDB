/*
 ***********************************************************************
 * Copyright 2019 VMware, Inc.  All rights reserved. VMware Confidential
 ***********************************************************************
 */

package org.corfudb.utils.lock;


import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.utils.lock.LockDataTypes.LockId;
import org.corfudb.utils.lock.persistence.LockStore;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Applications can register interest for a lock using the LockClient. When a lock is acquired on behalf of an instance
 * of an application it is notified through registered callbacks. Similarly if a lock is lost/revoked the corresponding
 * application instance is notified through callbacks.
 *
 * @author mdhawan
 * @since 04/17/2020
 */
// TODO Figure out if the lock client should be a singleton
@Slf4j
public class LockClient {

    // all the locks that the applications are interested in.
    private static final Map<LockId, Lock> locks = new ConcurrentHashMap<>();

    /**
     * Context is used to provide access to common values and resources needed by objects implementing
     * the Lock functionality.
     */
    @Data
    public class ClientContext {

        private final UUID clientUuid;
        private final LockStore lockStore;
        private final ScheduledExecutorService taskScheduler;
        private final ExecutorService lockListenerExecutor;


        public ClientContext(UUID clientUuid, LockStore lockStore, ScheduledExecutorService taskScheduler, ExecutorService lockListenerExecutor) {
            this.clientUuid = clientUuid;
            this.lockStore = lockStore;
            this.taskScheduler = taskScheduler;
            this.lockListenerExecutor = lockListenerExecutor;
        }
    }
    // The context contains objects that are shared across the locks in this client.
    private final ClientContext clientContext;

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

        ScheduledExecutorService taskScheduler = Executors.newScheduledThreadPool(1, (r) ->
        {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("LockTaskThread");
            t.setDaemon(true);
            return t;
        });

        ExecutorService lockListenerExecutor = Executors.newFixedThreadPool(1, (r) ->
        {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("LockListenerThread");
            t.setDaemon(true);
            return t;
        });

        clientContext = new ClientContext(clientId, new LockStore(corfuRuntime), taskScheduler, lockListenerExecutor);
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

        locks.computeIfAbsent(
                lockId,
                (key) -> new Lock(lockId, lockListener, clientContext));
    }
}
