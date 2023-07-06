package org.corfudb.runtime.concurrent;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.corfudb.runtime.view.SMRObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by dmalkhi on 3/17/17.
 */
@Slf4j
public class StreamSeekAtomicityTest extends AbstractTransactionsTest {
    @Override
    public void TXBegin() { OptimisticTXBegin(); }

    protected int numIterations = PARAMETERS.NUM_ITERATIONS_LOW;

    /**
     * This test verifies commit atomicity against concurrent -read- activity,
     * which constantly causes rollbacks and optimistic-rollbacks.
     *
     * @throws Exception error
     */
    @Test
    public void ckCommitAtomicity() throws Exception {
        // Note: PersistentCorfuTable and MVO do not perform rollbacks
        final String tableName = "testTableA";
        PersistentCorfuTable<Long, Long> testTable = getRuntime().getObjectsView()
                .build()
                .setStreamName(tableName)
                .setTypeToken(PersistentCorfuTable.<Long, Long>getTableType())
                .open();

        CountDownLatch l1 = new CountDownLatch(2);
        AtomicBoolean commitDone = new AtomicBoolean(false);
        final int NTHREADS = 3;

        scheduleConcurrently(t -> {

            int txCnt;
            for (txCnt = 0; txCnt < numIterations; txCnt++) {
                TXBegin();

                // on first iteration, wait for all to start;
                // other iterations will proceed right away
                l1.await();

                // generate optimistic mutation
                testTable.insert(1L, (long)txCnt);

                // wait for it to be undon
                TXEnd();
            }
            // signal done
            commitDone.set(true);

            Assert.assertEquals((long)(txCnt-1), (long) testTable.get(1L));
        });

        // thread that keeps affecting optimistic-rollback of the above thread
        scheduleConcurrently(t -> {
            TXBegin();
            testTable.get(1L);

            // signal that transaction has started and obtained a snapshot
            l1.countDown();

            // keep accessing the snapshot, causing optimistic rollback

            while (!commitDone.get()){
                testTable.get(1L);
            }
        });

        // thread that keeps syncing with the tail of log
        scheduleConcurrently(t -> {
            // signal that thread has started
            l1.countDown();

            // keep updating the in-memory proxy from the log
            while (!commitDone.get() ){
                testTable.get(1L);
            }
        });

        executeScheduled(NTHREADS, PARAMETERS.TIMEOUT_NORMAL);
    }

    /**
     * This test is similar to above, but with multiple maps.
     * It verifies commit atomicity against concurrent -read- activity,
     * which constantly causes rollbacks and optimistic-rollbacks.
     *
     * @throws Exception error
     */
    @Test
    public void ckCommitAtomicity2() throws Exception {
        // Note: PersistentCorfuTable and MVO do not perform rollbacks
        final String tableName1 = "testTableA";
        PersistentCorfuTable<Long, Long> testTable1 = getRuntime().getObjectsView()
                .build()
                .setStreamName(tableName1)
                .setTypeToken(PersistentCorfuTable.<Long, Long>getTableType())
                .open();

        final String tableName2 = "testTableB";
        PersistentCorfuTable<Long, Long> testTable2 = getRuntime().getObjectsView()
                .build()
                .setStreamName(tableName2)
                .setTypeToken(PersistentCorfuTable.<Long, Long>getTableType())
                .open();

        CountDownLatch l1 = new CountDownLatch(2);
        AtomicBoolean commitDone = new AtomicBoolean(false);
        final int NTHREADS = 3;

        scheduleConcurrently(t -> {

            int txCnt;
            for (txCnt = 0; txCnt < numIterations; txCnt++) {
                TXBegin();

                // on first iteration, wait for all to start;
                // other iterations will proceed right away
                l1.await();

                // generate optimistic mutation
                testTable1.insert(1L, (long)txCnt);
                if (txCnt % 2 == 0)
                    testTable2.insert(1L, (long)txCnt);

                // wait for it to be undon
                TXEnd();
            }
            // signal done
            commitDone.set(true);

            Assert.assertEquals((long)(txCnt-1), (long) testTable1.get(1L));
            Assert.assertEquals((long)( (txCnt-1) % 2 == 0 ? (txCnt-1) : txCnt-2), (long) testTable2.get(1L));
        });

        // thread that keeps affecting optimistic-rollback of the above thread
        scheduleConcurrently(t -> {
            TXBegin();
            testTable1.get(1L);

            // signal that transaction has started and obtained a snapshot
            l1.countDown();

            // keep accessing the snapshot, causing optimistic rollback

            while (!commitDone.get()){
                testTable1.get(1L);
                testTable2.get(1L);
            }
        });

        // thread that keeps syncing with the tail of log
        scheduleConcurrently(t -> {
            // signal that thread has started
            l1.countDown();

            // keep updating the in-memory proxy from the log
            while (!commitDone.get() ){
                testTable1.get(1L);
                testTable2.get(1L);
            }
        });

        executeScheduled(NTHREADS, PARAMETERS.TIMEOUT_NORMAL);
    }

    /**
     * This test is similar to above, but with concurrent -write- activity
     *
     * @throws Exception error
     */
    @Test
    public void ckCommitAtomicity3() throws Exception {
        // Note: PersistentCorfuTable and MVO do not perform rollbacks
        final String tableName1 = "testTableA";
        PersistentCorfuTable<Long, Long> testTable1 = getRuntime().getObjectsView()
                .build()
                .setStreamName(tableName1)
                .setTypeToken(PersistentCorfuTable.<Long, Long>getTableType())
                .open();

        final String tableName2 = "testTableB";
        PersistentCorfuTable<Long, Long> testTable2 = getRuntime().getObjectsView()
                .build()
                .setStreamName(tableName2)
                .setTypeToken(PersistentCorfuTable.<Long, Long>getTableType())
                .open();

        CountDownLatch l1 = new CountDownLatch(2);
        AtomicBoolean commitDone = new AtomicBoolean(false);
        final int NTHREADS = 3;

        scheduleConcurrently(t -> {

            int txCnt;
            for (txCnt = 0; txCnt < numIterations; txCnt++) {
                TXBegin();

                // on first iteration, wait for all to start;
                // other iterations will proceed right away
                l1.await();

                // generate optimistic mutation
                testTable1.insert(1L, (long)txCnt);
                if (txCnt % 2 == 0)
                    testTable2.insert(1L, (long)txCnt);

                // wait for it to be undon
                TXEnd();
            }
            // signal done
            commitDone.set(true);

            Assert.assertEquals((long)(txCnt-1), (long) testTable1.get(1L));
            Assert.assertEquals((long)( (txCnt-1) % 2 == 0 ? (txCnt-1) : txCnt-2), (long) testTable2.get(1L));
        });

        // thread that keeps affecting optimistic-rollback of the above thread
        scheduleConcurrently(t -> {
            long specialVal = numIterations + 1;

            TXBegin();
            testTable1.get(1L);

            // signal that transaction has started and obtained a snapshot
            l1.countDown();

            // keep accessing the snapshot, causing optimistic rollback

            while (!commitDone.get()){
                testTable1.insert(1L, specialVal);
                testTable2.insert(1L, specialVal);
            }
        });

        // thread that keeps syncing with the tail of log
        scheduleConcurrently(t -> {
            // signal that thread has started
            l1.countDown();

            // keep updating the in-memory proxy from the log
            while (!commitDone.get() ){
                testTable1.get(1L);
                testTable2.get(1L);
            }
        });

        executeScheduled(NTHREADS, PARAMETERS.TIMEOUT_NORMAL);
    }
}
