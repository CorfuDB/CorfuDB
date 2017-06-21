package org.corfudb.runtime.concurrent;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.corfudb.util.CoopScheduler;
import org.corfudb.util.CoopUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.util.CoopScheduler.sched;

/**
 * Created by dmalkhi on 3/17/17.
 */
@Slf4j
public class StreamSeekAtomicityTest extends AbstractTransactionsTest {
    @Override
    public void TXBegin() {
        OptimisticTXBegin();
    }

    protected int numIterations = PARAMETERS.NUM_ITERATIONS_LOW;
    private String scheduleString;

    public static void main(String[] argv) {
        try {
            StreamSeekAtomicityTest t = new StreamSeekAtomicityTest();
            // @Before items
            t.clearTestStatus();
            t.setupScheduledThreads();
            t.resetThreadingTest();
            t.InitSM();
            t.resetTests();
            t.becomeCorfuApp();

            // Do real work
            t.ckCommitAtomicity();
            System.exit(0);
        } catch (Exception e) {
            System.err.printf("ERROR: Caught exception %s at:\n", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * This test verifies commit atomicity against concurrent -read- activity,
     * which constantly causes rollbacks and optimistic-rollbacks.
     *
     * @throws Exception
     */
    @Test
    public void ckCommitAtomicity() throws Exception {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            ckCommitAtomicity(i);
        }
    }

    public void ckCommitAtomicity(int iter) throws Exception {
        String mapName1 = "testMapA" + iter;
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        AtomicInteger l1 = new AtomicInteger(0);
        AtomicBoolean commitDone = new AtomicBoolean(false);
        final int NTHREADS = 3;
        CoopUtil m = setupCoopSchedule(NTHREADS);
        final int itersLimit = 100;

        m.scheduleCoopConcurrently((thr, t) -> {
            int txCnt;
            for (txCnt = 0; txCnt < numIterations; txCnt++) {
                TXBegin();

                // on first iteration, wait for all to start;
                // other iterations will proceed right away
                CoopUtil.barrierAwait(l1, 2);

                // generate optimistic mutation
                sched();
                //System.err.printf("0");
                testMap1.put(1L, (long) txCnt);

                // wait for it to be undon
                TXEnd();
            }
            // System.err.printf("0 finished,");
            // signal done
            commitDone.set(true);

            assertThat(testMap1.get(1L))
                    .describedAs(scheduleString)
                    .isEqualTo((long) (txCnt - 1));
            Assert.assertEquals((long) (txCnt - 1), (long) testMap1.get(1L));
        });

        // thread that keeps affecting optimistic-rollback of the above thread
        m.scheduleCoopConcurrently((thr, t) -> {
            int i = 0;

            TXBegin();
            sched();
            testMap1.get(1L);

            // signal that transaction has started and obtained a snapshot
            sched();
            CoopUtil.barrierCountdown(l1);
            sched();

            // keep accessing the snapshot, causing optimistic rollback

            while (!commitDone.get() && i++ < itersLimit) {
                sched();
                //System.err.printf("1");
                testMap1.get(1L);
            }
            // System.err.printf("1 finished,");
        });

        // thread that keeps syncing with the tail of log
        m.scheduleCoopConcurrently((thr, t) -> {
            int i = 0;

            // signal that thread has started
            sched();
            CoopUtil.barrierCountdown(l1);
            sched();

            // keep updating the in-memory proxy from the log
            while (!commitDone.get() && i++ < itersLimit) {
                sched();
                //System.err.printf("2");
                testMap1.get(1L);
            }
            // System.err.printf("2 finished,");
        });

        BiConsumer whenFails = (exception, failedSignal) -> {
            System.err.printf("MY: executeScheduled error by thr %s: %s\n", Thread.currentThread().getName(), exception);
            ((Exception) exception).printStackTrace();
            System.err.printf("MY: done\n");
            if (exception instanceof TransactionAbortedException) {
                System.err.printf("MY: TAE, skip...\n");
            } else {
                ((AtomicBoolean) failedSignal).set(true);
            }
        };
        boolean failed = m.executeScheduled(whenFails);
        if (failed) {
            throw new RuntimeException("CoopScheduler thread failed");
        }
    }

    /**
     * This test is similar to above, but with multiple maps.
     * It verifies commit atomicity against concurrent -read- activity,
     * which constantly causes rollbacks and optimistic-rollbacks.
     *
     * @throws Exception
     */
    @Test
    public void ckCommitAtomicity2() throws Exception {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            ckCommitAtomicity2(i);
        }
    }

    public void ckCommitAtomicity2(int iter) throws Exception {
        String mapName1 = "testMapA" + iter;
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        String mapName2 = "testMapB" + iter;
        Map<Long, Long> testMap2 = instantiateCorfuObject(SMRMap.class, mapName2);

        AtomicInteger l1 = new AtomicInteger(2);
        AtomicBoolean commitDone = new AtomicBoolean(false);
        final int NTHREADS = 3;
        CoopUtil m = setupCoopSchedule(NTHREADS);

        m.scheduleCoopConcurrently((thr, t) -> {
            int txCnt;
            for (txCnt = 0; txCnt < numIterations; txCnt++) {
                TXBegin();

                // on first iteration, wait for all to start;
                // other iterations will proceed right away
                CoopUtil.barrierAwait(l1, 2);

                // generate optimistic mutation
                sched();
                testMap1.put(1L, (long) txCnt);
                if (txCnt % 2 == 0) {
                    sched();
                    testMap2.put(1L, (long) txCnt);
                }

                // wait for it to be undon
                sched();
                TXEnd();
            }
            // signal done
            commitDone.set(true);

            assertThat((long) testMap1.get(1L))
                    .describedAs(scheduleString)
                    .isEqualTo((long) (txCnt - 1));
            assertThat((long) testMap2.get(1L))
                    .describedAs(scheduleString)
                    .isEqualTo((long) ((txCnt - 1) % 2 == 0 ? (txCnt - 1) : txCnt - 2));
        });

        // thread that keeps affecting optimistic-rollback of the above thread
        m.scheduleCoopConcurrently((thr, t) -> {
            TXBegin();
            sched();
            testMap1.get(1L);

            // signal that transaction has started and obtained a snapshot
            CoopUtil.barrierCountdown(l1);

            // keep accessing the snapshot, causing optimistic rollback

            sched();
            while (!commitDone.get()) {
                sched();
                testMap1.get(1L);
                sched();
                testMap2.get(1L);
            }
        });

        // thread that keeps syncing with the tail of log
        m.scheduleCoopConcurrently((thr, t) -> {
            // signal that thread has started
            CoopUtil.barrierCountdown(l1);

            // keep updating the in-memory proxy from the log
            sched();
            while (!commitDone.get()) {
                sched();
                testMap1.get(1L);
                sched();
                testMap2.get(1L);
            }
        });

        m.executeScheduled();
    }

    /**
     * This test is similar to above, but with concurrent -write- activity
     *
     * @throws Exception
     */
    @Test
    public void ckCommitAtomicity3() throws Exception {
    for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            ckCommitAtomicity3(i);
        }
    }

    public void ckCommitAtomicity3(int iter) throws Exception {
        String mapName1 = "testMapA" + iter;
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        String mapName2 = "testMapB" + iter;
        Map<Long, Long> testMap2 = instantiateCorfuObject(SMRMap.class, mapName2);

        AtomicInteger l1 = new AtomicInteger(2);
        AtomicBoolean commitDone = new AtomicBoolean(false);
        final int NTHREADS = 3;
        CoopUtil m = setupCoopSchedule(NTHREADS);

        m.scheduleCoopConcurrently((thr, t) -> {
            int txCnt;
            for (txCnt = 0; txCnt < numIterations; txCnt++) {
                TXBegin();

                // on first iteration, wait for all to start;
                // other iterations will proceed right away
                CoopUtil.barrierAwait(l1, 2);

                // generate optimistic mutation
                sched();
                testMap1.put(1L, (long) txCnt);
                if (txCnt % 2 == 0) {
                    sched();
                    testMap2.put(1L, (long) txCnt);
                }

                // wait for it to be undone
                sched();
                TXEnd();
            }
            // signal done
            commitDone.set(true);

            assertThat((long) testMap1.get(1L)).
                    describedAs(scheduleString)
                    .isEqualTo((long) (txCnt - 1));
            assertThat((long) testMap2.get(1L))
                    .describedAs(scheduleString)
                    .isEqualTo((long) ((txCnt - 1) % 2 == 0 ? (txCnt - 1) : txCnt - 2));
        });

        // thread that keeps affecting optimistic-rollback of the above thread
        m.scheduleCoopConcurrently((thr, t) -> {
            long specialVal = numIterations + 1;

            sched();
            TXBegin();
            sched();
            testMap1.get(1L);

            // signal that transaction has started and obtained a snapshot
            CoopUtil.barrierCountdown(l1);

            // keep accessing the snapshot, causing optimistic rollback

            sched();
            while (!commitDone.get()) {
                sched();
                testMap1.put(1L, specialVal);
                sched();
                testMap2.put(1L, specialVal);
            }
        });

        // thread that keeps syncing with the tail of log
        m.scheduleCoopConcurrently((thr, t) -> {
            // signal that thread has started
            CoopUtil.barrierCountdown(l1);

            // keep updating the in-memory proxy from the log
            sched();
            while (!commitDone.get()) {
                sched();
                testMap1.get(1L);
                sched();
                testMap2.get(1L);
            }
        });

        m.executeScheduled();
    }

    private CoopUtil setupCoopSchedule(int nThreads) {
        final int schedLength = 300;
        int[] schedule = CoopScheduler.makeSchedule(nThreads, schedLength);
        schedule = new int[]{0,0,1,1,1,2,2,2,2,1,1,1,2,2,1,2,0,2,1,0,1,2,1,1,2,2,1,1,1,1,1,1,2,
                0,2,2,0,0,0,1,0,0,0,1,0,1,0,1,1,0,1,0,2,1,0,2,0,1,0,1,1,2,1,0,2,1,1,2,0,2,0,2,0,1,0,1,2
                ,1,0,1,2,2,1,2,2,2,2,2,1,2,0,0,0,0,1,2,1,1,0,1,2,0,2,0,0,1,1,1,2,2,1,0,1,0,0,1,0,1,1,2,
                1,1,2,2,2,1,0,0,0,0,0,0,2,0,1,2,2,2,1,0,1,1,2,0,1,1,1,2,0,1,2,0,0,2,2,0,1,1,1,2,0,2,1,0
                ,1,0,2,2,1,1,0,0,2,0,1,1,0,0,1,2,2,1,0,1,1,2,2,1,0,1,1,0,2,0,0,1,1,1,0,0,2,2,0,1,0,2,2,
                0,0,2,0,1,1,2,2,0,2,0,2,2,1,2,2,2,1,1,1,2,1,1,2,0,0,0,1,2,1,2,1,2,0,1,2,2,2,2,1,2,1,0,2
                ,0,2,2,2,1,0,1,1,2,1,2,2,1,2,0,2,2,1,1,0,1,1,0,0,1,2,2,2,2,2,1,2,1,1,2,0,2,0,0,0,1,0,2,
                1,1,0,0,2,1};
        scheduleString = "Schedule is: " + CoopScheduler.formatSchedule(schedule);
        CoopScheduler.reset(nThreads);
        CoopScheduler.setSchedule(schedule);
        return new CoopUtil();
    }
}