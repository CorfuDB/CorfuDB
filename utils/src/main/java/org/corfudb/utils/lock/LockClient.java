/*
 ***********************************************************************
 * Copyright 2019 VMware, Inc.  All rights reserved. VMware Confidential
 ***********************************************************************
 */

package org.corfudb.utils.lock;


import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.ExampleSchemas.LockOp;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.utils.CommonTypes;
import org.corfudb.utils.lock.LockDataTypes.LockId;
import org.corfudb.utils.lock.persistence.LockStore;
import org.corfudb.utils.lock.states.LockEvent;
import org.corfudb.utils.lock.states.LockStateType;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.*;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

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

    private static final int SINGLE_THREAD = 1;

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

    private final ScheduledExecutorService leaseMonitorScheduler;

    private final TestLockEventListener testLockEventListener;

    // duration between monitoring runs
    @Setter
    private static int durationBetweenLockMonitorRuns = 10;

    private final int shutdownWaitTime = 100;

    // The context contains objects that are shared across the locks in this client.
    private final ClientContext clientContext;

    private static final String LOCK_OP_TABLE_NAME = "LOCK_OP";

    @Getter
    private final UUID clientId;

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

        this.taskScheduler = Executors.newScheduledThreadPool(SINGLE_THREAD, runnable ->
        {
            Thread t = Executors.defaultThreadFactory().newThread(runnable);
            t.setName("LockTaskThread");
            t.setDaemon(true);
            return t;
        });

        // Single threaded scheduler to monitor the acquired locks (lease)
        // A dedicated scheduler is required in case the task scheduler is stuck in some database operation
        // and the previous lock owner can effectively expire the lock.
        leaseMonitorScheduler = Executors.newScheduledThreadPool(SINGLE_THREAD, runnable ->
        {
            Thread t = Executors.defaultThreadFactory().newThread(runnable);
            t.setName("LeaseMonitorThread");
            t.setDaemon(true);
            return t;
        });

        this.lockListenerExecutor = Executors.newFixedThreadPool(SINGLE_THREAD, runnable ->
        {
            Thread t = Executors.defaultThreadFactory().newThread(runnable);
            t.setName("LockListenerThread");
            t.setDaemon(true);
            return t;
        });

        this.lockMonitorScheduler = Executors.newScheduledThreadPool(SINGLE_THREAD, runnable ->
        {
            Thread t = Executors.defaultThreadFactory().newThread(runnable);
            t.setName("LockMonitorThread");
            t.setDaemon(true);
            return t;
        });

        this.clientId = clientId;
        this.lockStore = new LockStore(corfuRuntime, clientId);
        this.clientContext = new ClientContext(clientId, lockStore, taskScheduler, lockListenerExecutor, leaseMonitorScheduler);
        testLockEventListener = new TestLockEventListener(this, corfuRuntime);
        try {
            lockStore.getCorfuStore().openTable(
                CORFU_SYSTEM_NAMESPACE, LOCK_OP_TABLE_NAME,
                LockOp.class, LockOp.class, null,
                TableOptions.fromProtoSchema(LockOp.class));
            long trimMark =
                corfuRuntime.getLayoutView().getRuntimeLayout().getLogUnitClient(corfuRuntime.getLayoutServers().get(0)).getTrimMark().get();
            CorfuStoreMetadata.Timestamp ts = CorfuStoreMetadata.Timestamp.newBuilder()
                .setEpoch(corfuRuntime.getLayoutView().getRuntimeLayout().getLayout().getEpoch())
                .setSequence(trimMark).build();
            lockStore.getCorfuStore().subscribeListener(testLockEventListener,
                CORFU_SYSTEM_NAMESPACE, "lock_test", ts);
        } catch(ExecutionException | InterruptedException e) {
            log.error("Could not start listener", e);
        }
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
                key -> new Lock(lockId, lockListener, clientContext));

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
                            log.info("LockClient: lease revoked for lock {}",
                                lockId.getLockName());
                            locks.get(lockId).input(LockEvent.LEASE_REVOKED);
                        }
                    } catch (Exception ex) {
                        log.error("Caught exception while monitoring locks.", ex);
                    }
                },
                durationBetweenLockMonitorRuns,
                durationBetweenLockMonitorRuns,
                TimeUnit.SECONDS

        );
    }

    public void shutdown() {
        log.info("Shutdown Lock Client");
        this.lockMonitorScheduler.shutdown();
        try {
            this.lockMonitorScheduler.awaitTermination(shutdownWaitTime,
                TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            this.lockMonitorScheduler.shutdownNow();
        }

        this.taskScheduler.shutdown();
        try {
            this.taskScheduler.awaitTermination(shutdownWaitTime,
                TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            this.taskScheduler.shutdownNow();
        }

        this.lockListenerExecutor.shutdown();
        try {
            this.lockListenerExecutor.awaitTermination(shutdownWaitTime,
                TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            this.lockListenerExecutor.shutdownNow();
        }

        this.leaseMonitorScheduler.shutdown();
        try {
            this.leaseMonitorScheduler.awaitTermination(shutdownWaitTime,
                TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            this.leaseMonitorScheduler.shutdownNow();
        }
        lockStore.getCorfuStore().unsubscribeListener(testLockEventListener);
    }

    /**
     * Context is used to provide access to common values and resources needed by objects implementing
     * the Lock functionality.
     */
    @Data
    public static class ClientContext {

        private final UUID clientUuid;
        private final LockStore lockStore;
        private final ScheduledExecutorService taskScheduler;
        private final ScheduledExecutorService leaseMonitorScheduler;
        private final ExecutorService lockListenerExecutor;

        public ClientContext(UUID clientUuid, LockStore lockStore, ScheduledExecutorService taskScheduler,
                             ExecutorService lockListenerExecutor,
                             ScheduledExecutorService leaseMonitorScheduler) {
            this.clientUuid = clientUuid;
            this.lockStore = lockStore;
            this.taskScheduler = taskScheduler;
            this.lockListenerExecutor = lockListenerExecutor;
            this.leaseMonitorScheduler = leaseMonitorScheduler;
        }
    }

    private static class TestLockEventListener implements StreamListener {
        private LockClient lockClient;
        private CorfuStore corfuStore;
        private static final String LOCK_GROUP = "Log_Replication_Group";
        private static final String LOCK_NAME = "Log_Replication_Lock";
        private LockId lockId = LockDataTypes.LockId.newBuilder()
            .setLockGroup(LOCK_GROUP).setLockName(LOCK_NAME).build();

        private static final String NAMESPACE = CORFU_SYSTEM_NAMESPACE;
        private static final String TABLE_NAME = "LOCK";

        private final Table<LockId, LockDataTypes.LockData, Message> table;

        TestLockEventListener(LockClient lockClient,
            CorfuRuntime corfuRuntime) throws NoSuchMethodException,
            InvocationTargetException, IllegalAccessException {
            this.lockClient = lockClient;
            corfuStore = new CorfuStore(corfuRuntime);
            this.table = this.corfuStore.openTable(NAMESPACE,
                TABLE_NAME,
                LockId.class,
                LockDataTypes.LockData.class,
                null,
                TableOptions.builder().build());
        }

        @Override
        @SuppressWarnings("printLine")
        public void onNext(CorfuStreamEntries results) {

            String flipOp = "Flip Lock";
            CorfuStreamEntry entry =
                results.getEntries().values().stream().findFirst()
                .map(corfuStreamEntries -> corfuStreamEntries.get(0))
                .orElse(null);
            
            if (Objects.equals(((LockOp)entry.getKey()).getOp(), flipOp)) {
                if (lockClient.getLocks().get(lockId).getState().getType() == LockStateType.HAS_LEASE) {
                    System.out.println("LockState: " + lockClient.getLocks().get(lockId).getState().getType() + " Revoking Lock");
                    lockClient.getLocks().get(lockId).input(LockEvent.LEASE_REVOKED);
                } else {
                    System.out.println("LockState: " + lockClient.getLocks().get(lockId).getState().getType() + "Acquiring Lock");
                    CommonTypes.Uuid clientId = CommonTypes.Uuid.newBuilder()
                        .setMsb(lockClient.getClientId().getMostSignificantBits())
                        .setLsb(lockClient.getClientId().getLeastSignificantBits())
                        .build();

                    Optional<LockDataTypes.LockData> lockInDataStore = get(lockId);

                    int leaseAcquisitionNumber;

                    if (lockInDataStore.isPresent()) {
                          leaseAcquisitionNumber =
                              lockInDataStore.get().getLeaseAcquisitionNumber() + 1;
                    } else {
                        leaseAcquisitionNumber = 1;
                    }

                    LockDataTypes.LockData newLockData = LockDataTypes.LockData.newBuilder()
                        .setLockId(lockId)
                        .setLeaseOwnerId(clientId)
                        .setLeaseRenewalNumber(0)
                        .setLeaseAcquisitionNumber(leaseAcquisitionNumber)
                        .build();
                    update(lockId, newLockData);
                    lockClient.getLocks().get(lockId).input(LockEvent.LEASE_ACQUIRED);
                }
            }
        }

        private Optional<LockDataTypes.LockData> get(LockId lockId) {
            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                CorfuStoreEntry record = txn.getRecord(TABLE_NAME, lockId);
                txn.commit();
                if (record.getPayload() != null) {
                    return Optional.of((LockDataTypes.LockData) record.getPayload());
                } else {
                    return Optional.empty();
                }
            } catch (Exception e) {
                log.error("Lock: {} Exception during get.", lockId, e);
                return Optional.empty();
            }
        }

        private void update(LockId lockId, LockDataTypes.LockData lockData) {
            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                txn.putRecord(table, lockId, lockData, null);
                txn.commit();
            } catch (Exception e) {
                log.error("Lock: {} Exception during update.", lockId, e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            throw new IllegalStateException(
                "Exception in TestLockEventListener");
        }
    }
}
