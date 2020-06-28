package org.corfudb.integration;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.utils.lock.Lock;
import org.corfudb.utils.lock.LockClient;
import org.corfudb.utils.lock.LockDataTypes;
import org.corfudb.utils.lock.LockListener;
import org.corfudb.utils.lock.states.LockState;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.corfudb.integration.LockIT.LOCK_LEASE_DURATION;
import static org.corfudb.integration.LockIT.LOCK_TIME_CONSTANT;

@Slf4j
public class LockFlipIT extends AbstractIT {
    private static final int corfuPortNum = 9000;
    private static final String LOCK_GROUP = "Log_Replication_Group";
    private static final String LOCK_NAME = "Log_Replication_Lock";
    private static final String endpoint = "localhost" + ":" + corfuPortNum;
    public static Semaphore lockAcquiredSem;
    public static Semaphore lockReleaseSem;
    Process corfuServer;

    private void initialize() throws IOException {
        corfuServer = runServer(corfuPortNum, true);
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        runtime = CorfuRuntime.fromParameters(params).parseConfigurationString(endpoint).connect();

        LockState.setDurationBetweenLeaseRenewals(LOCK_TIME_CONSTANT);
        LockState.setMaxTimeForNotificationListenerProcessing(LOCK_TIME_CONSTANT);
        LockClient.setDurationBetweenLockMonitorRuns(LOCK_TIME_CONSTANT);
        Lock.setLeaseDuration(LOCK_LEASE_DURATION);
    }

    /**
     * Test with one lock client that regiserInterest, suspendInterest, and resumeInterest APIs.
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InterruptedException
     */
    @Test
    public void testSingleLockSuspend() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        initialize();

        lockAcquiredSem = new Semaphore(1);
        lockReleaseSem = new Semaphore(1);
        lockAcquiredSem.acquire();
        lockReleaseSem.acquire();

        UUID nodeId0 = UUID.randomUUID();
        LockClient lock0 = new LockClient(nodeId0, runtime);
        LockListener lockListener0 = new TestLockListener(nodeId0);


        lock0.registerInterest(LOCK_GROUP, LOCK_NAME, lockListener0);

        // Block till acquire the lock;
        lockAcquiredSem.acquire();

        // SuspendInterest will release the lock
        lock0.suspendInterest();

        // Block till release the lock;
        lockReleaseSem.acquire();

        // Resume interest;
        lock0.resumeInterest();

        // Block till acquire the lock again;
        lockAcquiredSem.acquire();

        lock0.stop();
        runtime.shutdown();
        corfuServer.destroy();
    }

    /**
     * Test with two lock client A and B, that one lock client that first holds the lock gives up the lock by calling
     * suspendInterest, verify that the other client will grab the lock.
     * Then later suspended lock client will resumeInterest, then enforce a lock flip again.
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InterruptedException
     */
    @Test
    public void testTwoLockFlip() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InterruptedException {
        initialize();

        lockAcquiredSem = new Semaphore(1);
        lockReleaseSem = new Semaphore(1);
        lockAcquiredSem.acquire();
        lockReleaseSem.acquire();

        UUID nodeId0 = UUID.randomUUID();
        LockClient lockClient0 = new LockClient(nodeId0, runtime);
        TestLockListener lockListener0= new TestLockListener(nodeId0);
        lockClient0.registerInterest(LOCK_GROUP, LOCK_NAME, lockListener0);

        // The lockClient0 acquire the lock
        lockAcquiredSem.acquire();
        assertThat(lockListener0.lockAquireCnt).isEqualTo(1);

        // Start lockClient1
        UUID nodeId1 = UUID.randomUUID();
        LockClient lockClient1 = new LockClient(nodeId1, runtime);
        TestLockListener lockListener1= new TestLockListener(nodeId1);
        lockClient1.registerInterest(LOCK_GROUP, LOCK_NAME, lockListener1);

        // lockClient0 release the lock
        lockClient0.suspendInterest();

        // One lock client will release the lock
        lockReleaseSem.acquire();
        assertThat(lockListener0.lockReleaseCnt).isEqualTo(1);

        // Another client will acquire the lock
        lockAcquiredSem.acquire();
        assertThat(lockListener1.lockAquireCnt).isEqualTo(1);

        // lockClient0 start to fetch the lock again.
        lockClient0.resumeInterest();

        // lockClient1 release the lock
        lockClient1.suspendInterest();

        // Verify the lock is released
        lockReleaseSem.acquire();

        // Very the lock is acquired
        lockAcquiredSem.acquire();

        assertThat(lockListener0.lockAquireCnt).isEqualTo(2);

        lockClient0.stop();
        lockClient1.stop();
        runtime.shutdown();
        corfuServer.destroy();
    }

    @Data
    static class TestLockListener implements LockListener {
        UUID uuid;

        public int lockAquireCnt = 0;
        public int lockReleaseCnt = 0;

        public TestLockListener(UUID uuid) {
            this.uuid = uuid;
        }

        @Override
        public void lockAcquired(LockDataTypes.LockId lockId) {
            lockAcquiredSem.release();
            lockAquireCnt++;
            log.info("Listener Lock acquired id={}", lockId);
        }

        @Override
        public void lockRevoked(LockDataTypes.LockId lockId) {
            lockReleaseSem.release();
            lockReleaseCnt++;
            log.info("Listener Lock released id={}", lockId);
        }
    }
}
