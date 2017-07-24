package org.corfudb.runtime.collections;

import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dmalkhi on 3/20/17.
 */
public class SMRMapEntrySetTest extends AbstractTransactionsTest {
    @Override
    public void TXBegin() { OptimisticTXBegin(); }

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    public CorfuRuntime r;


    @Before
    public void setRuntime() throws Exception {
        r = getDefaultRuntime().connect();
    }

    @Test
    public void manipulateSets()
            throws Exception {
        Map<Long, Long> testMap = instantiateCorfuObject(SMRMap.class,
                "mapsettest");


        // populate the map
        for (long i = 0; i < (long) PARAMETERS.NUM_ITERATIONS_LOW; i++)
            testMap.put(i, i);

        assertThat(testMap.get(0L))
                .isEqualTo(0L);
        assertThat(testMap.size())
                .isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW);

        // get a snapshot of the keys
        Set<Long> keys = testMap.keySet();
        assertThat(keys.size())
                .isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW);
        assertThat(keys.contains(0L))
                .isTrue();

        // manipulate the map, verify that keys set is unmodified,
        // the original map is modified
        testMap.remove(0L);

        assertThat(keys.size())
                .isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW);
        assertThat(keys.contains(0L))
                .isTrue();

        assertThat(testMap.containsKey(0L))
                .isFalse();
        assertThat(testMap.size())
                .isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW-1);

    }

    @Test
    public void manipulateSetsConcurrent()
            throws Exception {
        Map<Long, Long> testMap = instantiateCorfuObject(SMRMap.class,
                "mapsettest");
        CountDownLatch l1 = new CountDownLatch(1);
        CountDownLatch l2 = new CountDownLatch(1);
        CountDownLatch l3 = new CountDownLatch(1);

        // Block until sequencer operational.
        r.getSequencerView().nextToken(Collections.EMPTY_SET, 0);

        // first thread: create and manipulate map
        scheduleConcurrently(t -> {

            // signal start to second thread
            l1.countDown();

            // populate the map
            for (long i = 0; i < (long) PARAMETERS.NUM_ITERATIONS_LOW; i++)
                testMap.put(i, i);

            assertThat(testMap.get(0L))
                    .isEqualTo(0L);
            assertThat(testMap.size())
                    .isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW);

            // wait for second thread to advance
            l2.await();

            // manipulate the map, verify that keys set is unmodified,
            // the original map is modified
            testMap.remove(0L);

            assertThat(testMap.containsKey(0L))
                    .isFalse();
            assertThat(testMap.size())
                    .isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW - 1);

            // allow third thread to proceed
            l3.countDown();
        });

        // 2nd thread: get an immutable copy of the keys in the
        // at an arbitrary snapshot
        scheduleConcurrently(t -> {
            l1.await();

            // get a snapshot of the keys;
            // we don't know at which point the snapshot will be,
            // relative to the other thread
            Set<Long> keys = testMap.keySet();
            int snapshotSize = keys.size();

            if (snapshotSize > 0)
                assertThat(keys.contains(0L))
                        .isTrue();

            // signal that one snapshot was taken already
            l2.countDown();

            // verify that the immutable snapshot remains immutable
            while (l3.getCount() > 0) {
                assertThat(keys.size())
                        .isEqualTo(snapshotSize);
                if (snapshotSize > 0)
                    assertThat(keys.contains(0L))
                            .isTrue();
            }
            l3.await();
            assertThat(keys.size())
                    .isEqualTo(snapshotSize);
            if (snapshotSize > 0)
                assertThat(keys.contains(0L))
                        .isTrue();
        } );

        scheduleConcurrently(t -> {
            l3.await();

            // get a snapshot of the keys;
            Set<Long> keys = testMap.keySet();
            assertThat(keys.size())
                    .isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW-1);
            assertThat(keys.contains(0L))
                    .isFalse();
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_SHORT);

    }

}
