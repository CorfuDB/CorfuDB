package org.corfudb.utils;

import com.google.protobuf.Message;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.utils.lock.Lock;
import org.corfudb.utils.lock.LockDataTypes;
import org.corfudb.utils.lock.persistence.LockStore;
import org.corfudb.utils.lock.persistence.LockStoreException;
import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

public class LockStoreTest extends AbstractViewTest {

    private static final String LOCK_GROUP = "Test Group";
    private static final String LOCK_NAME = "Test Name";
    private static final int NUM_ITERATIONS = 50;
    private static final int NUM_ITERATIONS_SMALL = 10;
    private static final int TIMEOUT = 20;
    private static final String LOCK_TABLE_NAME = "LOCK";
    private static final int LOCK_LEASE_DURATION_SECONDS = 1;
    private static final int THOUSAND = 1000;
    private static final int TEN = 10;
    private boolean acquired1 = false;
    private boolean acquired2 = false;
    List<LockDataTypes.LockId> expiredLockIds1 = new ArrayList<>();
    List<LockDataTypes.LockId> expiredLockIds2 = new ArrayList<>();

    // Two runtimes try to acquire a lock with the same lock id concurrently.
    // Verify that only one of them succeeds.
    @Test
    public void testAcquireConcurrently() throws Exception {
        LockDataTypes.LockId lockId = LockDataTypes.LockId.newBuilder()
            .setLockGroup(LOCK_GROUP).setLockName(LOCK_NAME).build();
        CorfuRuntime runtime1 = getDefaultRuntime().connect();
        CorfuRuntime runtime2 = getNewRuntime(getDefaultNode()).connect();
        LockStore lockStore1 = new LockStore(runtime1, UUID.randomUUID());
        LockStore lockStore2 = new LockStore(runtime2, UUID.randomUUID());

        for (int i=0; i<NUM_ITERATIONS; i++) {
            scheduleConcurrently(f -> {
                try {
                    acquired1 = lockStore1.acquire(lockId);
                } catch (LockStoreException le) {
                    acquired1 = false;
                }
            });

            scheduleConcurrently(f -> {
                try {
                    acquired2 = lockStore2.acquire(lockId);
                } catch (LockStoreException le) {
                    acquired2 = false;
                }
            });
            executeScheduled(2, TIMEOUT, TimeUnit.SECONDS);
            Assert.assertTrue(acquired1 || acquired2);
            Assert.assertFalse(acquired1 && acquired2);

            clearLockTable(runtime1);
        }
    }

    private void clearLockTable(CorfuRuntime runtime) {
        CorfuStore corfuStore = new CorfuStore(runtime);
        Table<LockDataTypes.LockId, LockDataTypes.LockData, Message> lockTable =
            corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LOCK_TABLE_NAME);
        lockTable.clearAll();
    }

    // Two runtimes try to renew an acquired lock concurrently.  Verify that
    // only the current owner of the lock can renew it.
    @Test
    public void testRenewalBySameNode() throws Exception {
        LockDataTypes.LockId lockId = LockDataTypes.LockId.newBuilder()
            .setLockGroup(LOCK_GROUP).setLockName(LOCK_NAME).build();
        CorfuRuntime runtime1 = getDefaultRuntime().connect();
        CorfuRuntime runtime2 = getNewRuntime(getDefaultNode()).connect();
        CorfuStore corfuStore1 = new CorfuStore(runtime1);
        CorfuStore corfuStore2 = new CorfuStore(runtime2);
        LockStore lockStore1 = new LockStore(runtime1, UUID.randomUUID());
        LockStore lockStore2 = new LockStore(runtime2, UUID.randomUUID());

        for (int i = 0; i < NUM_ITERATIONS; i++) {
            scheduleConcurrently(f -> {
                try {
                    acquired1 = lockStore1.acquire(lockId);
                } catch (LockStoreException le) {
                    acquired1 = false;
                }
            });

            scheduleConcurrently(f -> {
                try {
                    acquired2 = lockStore2.acquire(lockId);
                } catch (LockStoreException le) {
                    acquired2 = false;
                }
            });
            executeScheduled(2, TIMEOUT, TimeUnit.SECONDS);
            Assert.assertTrue(acquired1 || acquired2);
            Assert.assertFalse(acquired1 && acquired2);

            TxnContext txn = corfuStore1.txn(CORFU_SYSTEM_NAMESPACE);
            LockDataTypes.LockData dataBeforeRenewal =
                lockStore1.get(lockId, txn).get();
            txn.commit();

            // Renew the lock
            scheduleConcurrently(f -> {
                try {
                    acquired1 = lockStore1.renew(lockId);
                } catch (LockStoreException le) {
                    acquired1 = false;
                }
            });
            scheduleConcurrently(f -> {
                try {
                    acquired2 = lockStore2.renew(lockId);
                } catch (LockStoreException le) {
                    acquired2 = false;
                }
            });
            executeScheduled(2, TIMEOUT, TimeUnit.SECONDS);
            Assert.assertTrue(acquired1 || acquired2);
            Assert.assertFalse(acquired1 && acquired2);

            txn = corfuStore2.txn(CORFU_SYSTEM_NAMESPACE);
            LockDataTypes.LockData dataAfterRenewal =
                lockStore1.get(lockId, txn).get();
            txn.commit();

            Assert.assertTrue(Objects.equals(
                dataBeforeRenewal.getLeaseOwnerId(),
                dataAfterRenewal.getLeaseOwnerId()));
            Assert.assertEquals(
                dataBeforeRenewal.getLeaseRenewalNumber()+1,
                dataAfterRenewal.getLeaseRenewalNumber());

            clearLockTable(runtime1);
        }
    }

    // Two runtimes find the lock with expired lease.  Verify that both get
    // the same result.
    @Test
    public void testFilterExpiredLocks() throws Exception {

        Lock.setLeaseDuration(LOCK_LEASE_DURATION_SECONDS);

        LockDataTypes.LockId lockId = LockDataTypes.LockId.newBuilder()
            .setLockGroup(LOCK_GROUP).setLockName(LOCK_NAME).build();

        CorfuRuntime runtime1 = getDefaultRuntime().connect();
        CorfuRuntime runtime2 = getNewRuntime(getDefaultNode()).connect();

        for (int i = 0; i < NUM_ITERATIONS_SMALL; i++) {
            // Need to create a new LockStore instance on every iteration to
            // avoid carrying forward the in-mem maps inside LockStore
            LockStore lockStore1 = new LockStore(runtime1, UUID.randomUUID());
            LockStore lockStore2 = new LockStore(runtime2, UUID.randomUUID());

            // Acquire a lock and verify that only a single client succeeds
            scheduleConcurrently(f -> {
                try {
                    acquired1 = lockStore1.acquire(lockId);
                } catch (LockStoreException le) {
                    acquired1 = false;
                }
            });

            scheduleConcurrently(f -> {
                try {
                    acquired2 = lockStore2.acquire(lockId);
                } catch (LockStoreException le) {
                    acquired2 = false;
                }
            });
            executeScheduled(2, TIMEOUT, TimeUnit.SECONDS);
            Assert.assertTrue(acquired1 || acquired2);
            Assert.assertFalse(acquired1 && acquired2);

            // Filter the locks with expired lease.  The first invocation
            // just adds the lock to the in-mem 'observedLocks' map inside
            // LockStore.  So this invocation will not detect the lock as
            // expired.
            scheduleConcurrently(f -> {
                expiredLockIds1.addAll(
                    lockStore1.filterLocksWithExpiredLeases(
                        Collections.singletonList(lockId)));
            });
            scheduleConcurrently(f -> {
                expiredLockIds2.addAll(lockStore2.filterLocksWithExpiredLeases(
                    Collections.singletonList(lockId)));
            });
            executeScheduled(2, TIMEOUT, TimeUnit.SECONDS);
            Assert.assertTrue(expiredLockIds1.isEmpty());
            Assert.assertTrue(expiredLockIds2.isEmpty());

            // Wait till the duration of the lock lease expiry + an
            // additional delay of 10ms.
            // LockStore detects a lock as expired if (the duration between
            // the time it was first read - lease expiry time) is before the
            // current system clock time.  So adding an arbitrary 10ms so
            // that clock skew is accounted for and the lock is detected as
            // expired.
            Thread.sleep(LOCK_LEASE_DURATION_SECONDS*THOUSAND+TEN);

            // Filter again and verify that the expired lock is returned
            scheduleConcurrently(f -> {
                expiredLockIds1.addAll(
                    lockStore1.filterLocksWithExpiredLeases(
                        Collections.singletonList(lockId)));
            });
            scheduleConcurrently(f -> {
                expiredLockIds2.addAll(lockStore2.filterLocksWithExpiredLeases(
                    Collections.singletonList(lockId)));
            });
            executeScheduled(2, TIMEOUT, TimeUnit.SECONDS);

            Assert.assertEquals(1, expiredLockIds1.size());
            Assert.assertEquals(1, expiredLockIds2.size());
            Assert.assertTrue(Objects.equals(expiredLockIds1.get(0),
                expiredLockIds2.get(0)));

            clearLockTable(runtime1);
            expiredLockIds1.clear();
            expiredLockIds2.clear();
        }
    }
}
