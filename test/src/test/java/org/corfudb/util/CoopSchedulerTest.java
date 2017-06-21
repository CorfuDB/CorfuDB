package org.corfudb.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.AbstractCorfuTest.PARAMETERS;
import static org.corfudb.util.CoopScheduler.sched;

/**
 *  Basic tests for the CoopScheduler.
 */
@Slf4j
public class CoopSchedulerTest  {
    private final int TEST_ITERS = 16;

    /**
     * Test the CoopScheduler under conditions where each scheduled thread
     * cannot block for arbitrary periods, e.g., due to side-effect of
     * obtaining a lock and also making scheduling decisions within the
     * critical region guarded by the lock.
     *
     * Our test will perform various degrees of "long" computations
     * based on Thread.sleep().  Such long computations should not cause
     * differences in thread scheduling by CoopScheduler.
     *
     * @throws Exception on failure: two test executions using the same
     * schedule has in fact created different execution traces.
     * An exception may also be thrown by the *Inner() function, which
     * performs additional sanity checking of the execution trace.
     */
    @Test
    public void testNonBlocked() throws Exception {
        final int ITERS = 5;

        ArrayList<Object[]> logs = new ArrayList<>();
        for (int i = 0; i < ITERS; i++) {
            logs.add(testNonBlockedInner());
        }
        assertThat(CoopScheduler.logsAreIdentical(logs)).isTrue();
    }

    /**
     * Execute a set of 7 threads with a known constant schedule under CoopScheduler.
     * Generate an execution trace that is verified for sanity properties, listed in
     * the 'throws' section below.
     *
     * We permit only one source of randomness: we assign random sleep intervals
     * to each worker thread, to simulate various degrees of long computation.
     * Long vs. short work execution steps should not alter any thread's actual
     * execution schedule.
     *
     * @return Execution trace log, for further sanity evaluation by the caller.
     * @throws Exception on failure: three conditions are checked:
     *   1. The execution trace is longer than the schedule (for bounds checking of #2)
     *   2. Given the log trace of thread execution order, the prefix of the trace
     *      is exactly equal to the desired schedule.
     *   3. Due to the unfairness of the schedule (T6 and T5 are the only threads
     *      that have only one tick, all other threads have multiple ticks), we expect
     *      that either T5 or T6 created the last entry in the trace.
     */
    public Object[] testNonBlockedInner() throws Exception {
        final int T0=0, T1=1, T2=2, T3=3, T4=4, T5=5, T6=6;
        int[] schedule = new int[] {T1,T1,T0,T2,T1,T0,T4,T3,T4,T3,T6,T5};
        final int eachSleep = 1;
        CoopScheduler.reset(T6+1 + 2); // add a few unused thread count for bug hunting
        CoopScheduler.setSchedule(schedule);

        Thread ts[] = new Thread[T6+1];
        for (int j = 0; j < ts.length; j++) {
            final int jj = j;
            ts[j] = new Thread(() -> threadWorkNonBlocked(jj, RandomUtils.nextInt(eachSleep)) );
        }
        for (int i = 0; i < ts.length; i++) { ts[i].start(); }
        CoopScheduler.runScheduler(ts.length);
        for (int i = 0; i < ts.length; i++) { ts[i].join(); }

        Object[] log = CoopScheduler.getLog();
        assertThat(log.length).isGreaterThan(schedule.length);
        assertThat(Arrays.copyOf(log, schedule.length)).isEqualTo(schedule);
        assertThat((int) log[log.length-1]).isGreaterThan(T4); // Last executed should be T5 or T6
        return CoopScheduler.getLog();
    }

    void threadWorkNonBlocked(int threadNum, int sleepTime) {
        final int ITERS = 5;

        try { Thread.sleep(sleepTime); } catch (Exception e) {}
        int t = CoopScheduler.registerThread(threadNum);
        if (t < 0) {
            System.err.printf("Thread registration failed, exiting");
            System.exit(1);
        }

        for (int i = 0; i < ITERS; i++) {
            sched();
            CoopScheduler.appendLog(threadNum);
            // System.err.printf("Thread %d iter (%d)", t, i);
            try { Thread.sleep(sleepTime); } catch (Exception e) {}
        }

        CoopScheduler.threadDone();
    }

    /**
     * Test the CoopScheduler under conditions where each scheduled thread
     * may/will block for arbitrary periods, e.g., due to side-effect of
     * obtaining a lock and also making scheduling decisions within the
     * critical region guarded by the lock.
     *
     * This test involves using Java object 'synchronized' access together
     * with making CoopScheduler decisions inside of the monitor/critical
     * region.  If we omit calls to withdraw() and rejoin(), we know this
     * test will deadlock.  If we timeout, we assume deadlock.
     *
     * Java doesn't enforce fairness when entering synchronized blocks.
     * Therefore we cannot expect all execution traces to be exactly
     * equal all the time.  We might check the trace that all entries
     * by any thread T's iteration I must must not be interspersed with
     * some T' thread's iteration I' ... but we won't.  We assume that
     * the JVM implements 'synchronized' blocks correctly.
     *
     * @throws Exception on failure: Deadlock is assumed if the wrapper
     * function detects execution time longer than an absurdly large
     * timeout value.
     */

    @Test
    public void testBlocked() throws Exception {
        final int ITERS = 5;

        ArrayList<Object[]> logs = new ArrayList<>();
        for (int i = 0; i < ITERS; i++) {
            CompletableFuture<Object[]> cf = CompletableFuture.supplyAsync(() -> {
                    return testBlockedInner();
            });
            Object[] log = cf.get(PARAMETERS.TIMEOUT_NORMAL.toMillis(), TimeUnit.MILLISECONDS);
            // If we get this far, then we didn't have a timeout exception, good.
            // However, we need to check if thread.join() had a problem & returned null.
            assertThat(log).isNotNull();
        }
    }

    public Object[] testBlockedInner() {
        final int T0=0, T1=1, T2=2;
        final int SCHED_LEN = 10;
        final int MAX_TICKS = 5;
        Object mutex = new Object();

        ArrayList<CoopScheduler.CoopQuantum> schedule = new ArrayList<>();
        for (int i = 0; i < SCHED_LEN; i++) {
            schedule.add(new CoopScheduler.CoopQuantum(RandomUtils.nextInt(T2+1),
                    RandomUtils.nextInt(MAX_TICKS)+1));
        }
        CoopScheduler.reset(T2+1);
        CoopScheduler.setSchedule(schedule);

        Thread ts[] = new Thread[T2+1];
        for (int j = 0; j < ts.length; j++) {
            final int jj = j;
            ts[j] = new Thread(() -> threadWorkBlocked(jj, mutex) );
        }
        for (int i = 0; i < ts.length; i++) { ts[i].start(); }
        CoopScheduler.runScheduler(ts.length);
        for (int i = 0; i < ts.length; i++) { try { ts[i].join(); } catch (Exception e) { return null; } }

        return CoopScheduler.getLog();
    }

    void threadWorkBlocked(int threadNum, Object mutex) {
        final int ITERS = 3;

        CoopScheduler.registerThread(threadNum);
        for (int i = 0; i < ITERS; i++) {
            sched();
            CoopScheduler.withdraw();
            synchronized (mutex) {
                CoopScheduler.rejoin();
                for (int j = 0; j < 2; j++) {
                    sched();
                    CoopScheduler.appendLog(threadNum);
                }
            }
            sched();
        }
        CoopScheduler.threadDone();
    }

    /** Run testCoopStampedLockInner() some number of times using
     *  the same schedule, and check that all execution log histories
     *  are exactly the same.
     */
    @Test
    public void testCoopStampedLock() throws Exception {
        final int ITERS = 5;
        final int T0 = 0, T1 = 1, T2 = 2, T3 = 3, T4 = 4, T5 = 5, T6 = 6;
        ArrayList<Object[]> logs = new ArrayList<>();

        for (int j = 0; j < ITERS; j++) {
            final int onehundred = 100;
            // Use a fixed schedule for j=0 iteration, and
            // use a random schedule for all others.
            int[] schedule = (j == 0) ?
                    new int[]{T1, T1, T0, T2, T1, T1, T1, T0, T4, T3, T4, T3, T3, T3, T6, T5} :
                    CoopScheduler.makeSchedule(T6 + 1, onehundred);

            failureDescription = new String("schedule = ");
            for (int k = 0; k < schedule.length; k++) {
                failureDescription += Integer.toString(schedule[k]) + ",";
            }

            for (int i = 0; i < ITERS; i++) {
                CoopScheduler.reset(T6 + 1);
                CoopScheduler.setSchedule(schedule);
                water = 0;
                logs.add(testCoopStampedLockInner(T6 + 1));
            }
            assertThat(CoopScheduler.logsAreIdentical(logs))
                    .describedAs(failureDescription)
                    .isTrue();
            logs.clear();
        }
    }

    public Object[] testCoopStampedLockInner(int numThreads) throws Exception {
        CoopStampedLock lock = new CoopStampedLock();
        Thread ts[] = new Thread[numThreads];

        for (int j = 0; j < ts.length; j++) {
            final int jj = j;
            if (j % 2 == 0) {
                ts[j] = new Thread(() -> threadWorkCoopStampedLock_LU(jj, lock));
            } else {
                ts[j] = new Thread(() -> threadWorkCoopStampedLock_try(jj, lock));
            }
        }
        for (int i = 0; i < ts.length; i++) { ts[i].start(); }
        CoopScheduler.runScheduler(ts.length);
        for (int i = 0; i < ts.length; i++) { ts[i].join(); }

        Object[] log = CoopScheduler.getLog();
        // printLog(log);
        return log;
    }

    private int water = 0;
    private String failureDescription;

    /** Stress the writeLock() and unlock() part of the StampedLock API.
     */
    private void threadWorkCoopStampedLock_LU(int tnum, CoopStampedLock lock) {
        int t = CoopScheduler.registerThread(tnum);
        assertThat(t)
                .describedAs(failureDescription)
                .isEqualTo(tnum);

        for (int i = 0; i < TEST_ITERS; i++) {
            sched();
            long ts = lock.writeLock();
            CoopScheduler.appendLog(t + "->" + ts);
            water++;
            sched();
            assertThat(water)
                    .describedAs(failureDescription)
                    .isEqualTo(1);
            water--;
            lock.unlock(ts);
        }

        CoopScheduler.threadDone(t);
    }

    /** Stress the tryOptimisticRead() and validate() part of the StampedLock API
     */
    private void threadWorkCoopStampedLock_try(int tnum, CoopStampedLock lock) {
        int t = CoopScheduler.registerThread(tnum);
        assertThat(t)
                .describedAs(failureDescription)
                .isEqualTo(tnum);

        for (int i = 0; i < TEST_ITERS; i++) {
            sched();
            long ts = lock.tryOptimisticRead();
            if (ts > 0) {
                int myWater = water + 1; // Simulate incrementing the water level.
                sched();
                if (lock.validate(ts)) {
                    assertThat(myWater)
                            .describedAs(failureDescription)
                            .isEqualTo(1);
                    CoopScheduler.appendLog(t + "}" + ts);
                } else {
                    CoopScheduler.appendLog(t + "X" + ts);
                }
            } else {
                CoopScheduler.appendLog(t + "Q");
            }
        }

        CoopScheduler.threadDone(t);
    }

    private void printLog(Object[] log) {
        System.err.printf("Log: ");
        for (int i = 0; i < log.length; i++) {
            System.err.printf("%s,", log[i]);
        }
        System.err.printf("\n");
    }
}
