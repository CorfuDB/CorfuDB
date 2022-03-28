package org.corfudb.utils;

import com.google.protobuf.Message;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.utils.lock.LockDataTypes;
import org.corfudb.utils.lock.persistence.LockStore;
import org.corfudb.utils.lock.persistence.LockStoreException;
import org.junit.Assert;
import org.junit.Test;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

public class LockStoreTest extends AbstractViewTest {

    private static final String LOCK_GROUP = "Test Group";
    private static final String LOCK_NAME = "Test Name";
    private static final int NUM_ITERATIONS = 50;
    private static final int TIMEOUT = 20;
    private static final String LOCK_TABLE_NAME = "LOCK";
    private boolean acquired1 = false;
    private boolean acquired2 = false;

    @Test
    public void testAcquire() throws Exception {
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

    @Test
    public void testRenewalBySameNode() throws Exception {
        LockDataTypes.LockId lockId = LockDataTypes.LockId.newBuilder()
            .setLockGroup(LOCK_GROUP).setLockName(LOCK_NAME).build();
        CorfuRuntime runtime1 = getDefaultRuntime().connect();
        CorfuRuntime runtime2 = getNewRuntime(getDefaultNode()).connect();
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

            LockDataTypes.LockData dataBeforeRenewal =
                lockStore1.get(lockId).get();

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

            LockDataTypes.LockData dataAfterRenewal =
                lockStore1.get(lockId).get();
            Assert.assertTrue(Objects.equals(
                dataBeforeRenewal.getLeaseOwnerId(),
                dataAfterRenewal.getLeaseOwnerId()));

            clearLockTable(runtime1);
        }
    }
}
