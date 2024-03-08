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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

public class LockStoreTest extends AbstractViewTest {

    private static final String LOCK_GROUP = "Test Group";
    private static final String LOCK_NAME = "Test Name";
    private static final int TASK_EXECUTE_TIMEOUT = 20;
    private static final String LOCK_TABLE_NAME = "LOCK";
    private static final int LOCK_LEASE_DURATION_LONG_SECONDS = 300;
    private static final int DELAY_MS = 10000;
    private boolean acquired1 = false;
    private boolean acquired2 = false;
    private boolean renewed1 = false;
    private boolean renewed2 = false;
    private List<LockDataTypes.LockId> expiredLockIds1 = new ArrayList<>();
    private List<LockDataTypes.LockId> expiredLockIds2 = new ArrayList<>();
    private CorfuRuntime runtime1;
    private CorfuRuntime runtime2;
    private final LockDataTypes.LockId lockId = LockDataTypes.LockId.newBuilder()
        .setLockGroup(LOCK_GROUP).setLockName(LOCK_NAME).build();

    @Before
    public void setUp() {
        runtime1 = getDefaultRuntime();
        runtime2 = getNewRuntime(getDefaultNode()).connect();
    }

    private void cleanupAfterIteration() {
        clearLockTable(runtime1);
        acquired1 = false;
        acquired2 = false;
        renewed1 = false;
        renewed2 = false;
        expiredLockIds1.clear();
        expiredLockIds2.clear();
    }

    @After
    public void cleanUp() {
        cleanupAfterIteration();
        runtime1.shutdown();
        runtime2.shutdown();
    }

    private void clearLockTable(CorfuRuntime runtime) {
        CorfuStore corfuStore = new CorfuStore(runtime);
        Table<LockDataTypes.LockId, LockDataTypes.LockData, Message> lockTable =
            corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LOCK_TABLE_NAME);
        lockTable.clearAll();
    }

    private void executeAcquireOrRenewConcurrently(LockStore lockStore1,
        LockStore lockStore2, boolean renew) throws Exception {

        if (renew) {
            scheduleConcurrently(f -> {
                try {
                    renewed1 = lockStore1.renew(lockId);
                } catch (LockStoreException le) {
                    renewed1 = false;
                }
            });
            scheduleConcurrently(f -> {
                try {
                    renewed2 = lockStore2.renew(lockId);
                } catch (LockStoreException le) {
                    renewed2 = false;
                }
            });
        } else {
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
        }
        executeScheduled(2, TASK_EXECUTE_TIMEOUT, TimeUnit.SECONDS);
    }

    // Two runtimes try to acquire a lock with the same lock id concurrently.
    // Verify that only one of them succeeds.
    // Repeat this operation multiple times to expose race in this workflow,
    // if any.  Each iteration clears the lock table and creates a new
    // LockStore instance to exercise the init scenario.
    @Test
    public void testAcquireConcurrently() throws Exception {
        for (int i=0; i<PARAMETERS.NUM_ITERATIONS_LOW; i++) {

            LockStore lockStore1 = new LockStore(runtime1, UUID.randomUUID());
            LockStore lockStore2 = new LockStore(runtime2, UUID.randomUUID());

            executeAcquireOrRenewConcurrently(lockStore1, lockStore2, false);

            Assert.assertTrue(acquired1 || acquired2);
            Assert.assertFalse(acquired1 && acquired2);

            cleanupAfterIteration();
        }
    }

    // Two runtimes try to acquire a lock with the same lock id concurrently.
    // Verify that only one of them succeeds.
    // Repeat this operation multiple times to expose race in this workflow,
    // if any.
    // Lock table is not cleared between iterations and the LockStore
    // instance is reused.  This test simulates the case where acquire() is
    // invoked even if there is a lock in the table(there is a lock owner).
    @Test
    public void testRepeatedAcquire() throws Exception {
        Lock.setLeaseDuration(LOCK_LEASE_DURATION_LONG_SECONDS);

        LockStore lockStore1 = new LockStore(runtime1, UUID.randomUUID());
        LockStore lockStore2 = new LockStore(runtime2, UUID.randomUUID());

        executeAcquireOrRenewConcurrently(lockStore1, lockStore2, false);

        Assert.assertTrue(acquired1 || acquired2);
        Assert.assertFalse(acquired1 && acquired2);

        CorfuStore corfuStore1 = new CorfuStore(runtime1);
        LockDataTypes.LockData existingLockData = null;
        try (TxnContext txn = corfuStore1.txn(CORFU_SYSTEM_NAMESPACE)) {
             existingLockData = lockStore1.get(lockId, txn).get();
            txn.commit();
        }

        for (int i=0; i<PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            executeAcquireOrRenewConcurrently(lockStore1, lockStore2, false);
            Assert.assertFalse(acquired1 || acquired2);

            LockDataTypes.LockData currentLockData = null;
            try (TxnContext txn = corfuStore1.txn(CORFU_SYSTEM_NAMESPACE)) {
                currentLockData = lockStore1.get(lockId, txn).get();
                txn.commit();
            }

            Assert.assertEquals(currentLockData.getLeaseOwnerId(),
                existingLockData.getLeaseOwnerId());
            Assert.assertTrue(Objects.equals(currentLockData, existingLockData));
        }
    }


    // Two runtimes try to renew an acquired lock concurrently.  Verify that
    // only one of them succeeds and only the current owner of the lock can
    // renew it.
    // Repeat this operation multiple times to expose race in this workflow,
    // if any.  Each iteration clears the lock table and creates a new
    // LockStore instance to exercise the (init+renew) scenario.
    @Test
    public void testRenewalBySameNode() throws Exception {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            LockStore lockStore1 = new LockStore(runtime1, UUID.randomUUID());
            LockStore lockStore2 = new LockStore(runtime2, UUID.randomUUID());

            executeAcquireOrRenewConcurrently(lockStore1, lockStore2, false);

            Assert.assertTrue(acquired1 || acquired2);
            Assert.assertFalse(acquired1 && acquired2);

            CorfuStore corfuStore1 = new CorfuStore(runtime1);
            LockDataTypes.LockData dataBeforeRenewal = null;
            try (TxnContext txn = corfuStore1.txn(CORFU_SYSTEM_NAMESPACE)) {
                dataBeforeRenewal = lockStore1.get(lockId, txn).get();
                txn.commit();
            }

            // Renew the lock
            executeAcquireOrRenewConcurrently(lockStore1, lockStore2, true);
            Assert.assertTrue(renewed1 || renewed2);
            Assert.assertFalse(renewed1 && renewed2);

            LockDataTypes.LockData dataAfterRenewal = null;
            try (TxnContext txn = corfuStore1.txn(CORFU_SYSTEM_NAMESPACE)) {
                dataAfterRenewal = lockStore1.get(lockId, txn).get();
                txn.commit();
            }

            Assert.assertTrue(Objects.equals(
                dataBeforeRenewal.getLeaseOwnerId(),
                dataAfterRenewal.getLeaseOwnerId()));
            Assert.assertEquals(
                dataBeforeRenewal.getLeaseRenewalNumber()+1,
                dataAfterRenewal.getLeaseRenewalNumber());

            cleanupAfterIteration();
        }
    }

    // Two runtimes find the lock with expired lease.  Verify that both get
    // the same result.
    // Repeat this operation multiple times to expose race in this workflow,
    // if any.  Each iteration clears the lock table and creates a new
    // LockStore instance to avoid carrying forward the in-mem
    // 'observedLocks' map inside LockStore.
    @Test
    public void testFilterExpiredLocks() throws Exception {

        Lock.setLeaseDuration((int)PARAMETERS.TIMEOUT_SHORT.getSeconds());

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            LockStore lockStore1 = new LockStore(runtime1, UUID.randomUUID());
            LockStore lockStore2 = new LockStore(runtime2, UUID.randomUUID());

            // Acquire a lock and verify that only a single client succeeds
            executeAcquireOrRenewConcurrently(lockStore1, lockStore2, false);
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
            executeScheduled(2, TASK_EXECUTE_TIMEOUT, TimeUnit.SECONDS);
            Assert.assertTrue(expiredLockIds1.isEmpty());
            Assert.assertTrue(expiredLockIds2.isEmpty());

            // Wait till the duration of the lock lease expiry + an
            // additional delay of 10ms.
            // LockStore detects a lock as expired if (the duration between
            // the time it was first read + lease expiry time) is before the
            // current system clock time.  So adding an arbitrary 10ms so
            // that the lock is detected as
            // expired.
            Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis() + DELAY_MS);

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
            executeScheduled(2, TASK_EXECUTE_TIMEOUT, TimeUnit.SECONDS);

            Assert.assertEquals(1, expiredLockIds1.size());
            Assert.assertEquals(1, expiredLockIds2.size());
            Assert.assertTrue(Objects.equals(expiredLockIds1.get(0),
                expiredLockIds2.get(0)));

            cleanupAfterIteration();
        }
    }

    /**
     * This test verifies the behavior of 2 operations when the lock table is empty.
     * 1)lock renewal and
     * 2)filtering of locks with expired lease
     * This table can be empty if no node has acquired leadership or it has been manually deleted.  The expected
     * behavior is for renewal to fail and filtering shows the requested lock as revocable so that the querying node
     * can acquire leadership.
     * @throws Exception
     */
    @Test
    public void testEmptyLockTable() throws Exception {
        LockStore lockStore = new LockStore(runtime1, UUID.randomUUID());

        // Renewal of a lock which has not yet been acquired is not valid
        Assert.assertFalse(lockStore.renew(lockId));

        // Get a list of locks with revocable lease.  As there is no lock currently acquired, the request lock can be
        // revoked.
        List<LockDataTypes.LockId> revocableLocks =
            lockStore.filterLocksWithExpiredLeases(Collections.singletonList(lockId)).stream().collect(Collectors.toList());
        Assert.assertEquals(1, revocableLocks.size());
        Assert.assertEquals(lockId, revocableLocks.get(0));
    }
}
