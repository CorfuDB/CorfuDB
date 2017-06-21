package org.corfudb.runtime.collections;

import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.corfudb.util.CoopScheduler;
import org.corfudb.util.CoopUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.util.CoopScheduler.sched;

/**
 * Variation of SMRMapEntrySetTest adapted to CoopScheduler use.
 */
public class SMRMapEntrySetCoopTest extends AbstractTransactionsTest {
    @Override
    public void TXBegin() { OptimisticTXBegin(); }

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    public CorfuRuntime r;

    private String scheduleString;

    @Before
    public void setRuntime() throws Exception {
        r = getDefaultRuntime().connect();
    }

    @Test
    public void manipulateSetsCoopConcurrent()
            throws Exception {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            manipulateSetsCoopConcurrent(i);
        }
    }

    public void manipulateSetsCoopConcurrent(int iter)
            throws Exception {
        Map<Long, Long> testMap = instantiateCorfuObject(SMRMap.class,
                "mapsettest" + iter);
        final int numThreads = 3;
        final int schedLength = 200;
        CoopUtil m = new CoopUtil();
        AtomicInteger l1 = new AtomicInteger(1);
        AtomicInteger l2 = new AtomicInteger(1);
        AtomicInteger l3 = new AtomicInteger(1);

        CoopScheduler.reset(numThreads);
        int[] schedule = CoopScheduler.makeSchedule(numThreads, schedLength);
        CoopScheduler.setSchedule(schedule);
        scheduleString = "Schedule is: " + CoopScheduler.formatSchedule(schedule);

        // first thread: create and manipulate map
        m.scheduleCoopConcurrently((t, thr) -> {

            // signal start to second thread
            l1.set(0);

            // populate the map
            for (long i = 0; i < (long) PARAMETERS.NUM_ITERATIONS_LOW; i++) {
                sched();
                testMap.put(i, i);
            }

            assertThat(testMap.get(0L))
                    .describedAs(scheduleString)
                    .isEqualTo(0L);
            sched();
            assertThat(testMap.size())
                    .describedAs(scheduleString)
                    .isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW);

            // wait for second thread to advance
            while (l2.get() > 0) { sched(); }

            // manipulate the map, verify that keys set is unmodified,
            // the original map is modified
            sched();
            testMap.remove(0L);

            sched();
            assertThat(testMap.containsKey(0L))
                    .describedAs(scheduleString)
                    .isFalse();
            sched();
            assertThat(testMap.size())
                    .describedAs(scheduleString)
                    .isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW - 1);

            // allow third thread to proceed
            l3.set(0);
        });

        // 2nd thread: get an immutable copy of the keys in the
        // at an arbitrary snapshot
        m.scheduleCoopConcurrently((t, thr) -> {
            while (l1.get() > 0) { sched(); }

            // get a snapshot of the keys;
            // we don't know at which point the snapshot will be,
            // relative to the other thread
            Set<Long> keys = testMap.keySet();
            // Without using the CoopScheduler, snapshotSize would
            // almost always be zero or one.
            int snapshotSize = keys.size();

            sched();
            if (snapshotSize > 0)
                assertThat(keys.contains(0L))
                        .describedAs(scheduleString)
                        .isTrue();

            // signal that one snapshot was taken already
            l2.set(0);

            // verify that the immutable snapshot remains immutable
            while (l3.get() > 0) {
                sched();
                assertThat(keys.size())
                        .describedAs(scheduleString)
                        .isEqualTo(snapshotSize);
                sched();
                if (snapshotSize > 0)
                    assertThat(keys.contains(0L))
                            .describedAs(scheduleString)
                            .isTrue();
            }
            while (l3.get() > 0) { sched(); }

            assertThat(keys.size())
                    .describedAs(scheduleString)
                    .isEqualTo(snapshotSize);
            sched();
            if (snapshotSize > 0)
                assertThat(keys.contains(0L))
                        .describedAs(scheduleString)
                        .isTrue();
        } );

        m.scheduleCoopConcurrently((t, thr) -> {
            while (l3.get() > 0) { sched(); }

            // get a snapshot of the keys;
            Set<Long> keys = testMap.keySet();
            assertThat(keys.size())
                    .describedAs(scheduleString)
                    .isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW-1);
            sched();
            assertThat(keys.contains(0L))
                    .describedAs(scheduleString)
                    .isFalse();
        });

        m.executeScheduled();

    }
}
