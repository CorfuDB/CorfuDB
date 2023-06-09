package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.corfudb.runtime.view.SMRObject;
import org.junit.Before;
import org.junit.Test;

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
    public void manipulateSets() throws Exception {
        PersistentCorfuTable<Long, Long> testTable = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("mapsettest")
                .setTypeToken(new TypeToken<PersistentCorfuTable<Long, Long>>() {})
                .open();

        // populate the map
        for (long i = 0; i < (long) PARAMETERS.NUM_ITERATIONS_LOW; i++)
            testTable.insert(i, i);

        assertThat(testTable.get(0L)).isEqualTo(0L);
        assertThat(testTable.size()).isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW);

        // get a snapshot of the keys
        Set<Long> keys = testTable.keySet();
        assertThat(keys.size()).isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW);
        assertThat(keys.contains(0L)).isTrue();

        // manipulate the map, verify that keys set is unmodified,
        // the original map is modified
        testTable.delete(0L);

        assertThat(keys.size()).isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW);
        assertThat(keys.contains(0L)).isTrue();

        assertThat(testTable.containsKey(0L)).isFalse();
        assertThat(testTable.size()).isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW - 1);
    }

    @Test
    public void manipulateSetsConcurrent() throws Exception {
        PersistentCorfuTable<Long, Long> testTable = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("mapsettest")
                .setTypeToken(new TypeToken<PersistentCorfuTable<Long, Long>>() {})
                .open();

        CountDownLatch l1 = new CountDownLatch(1);
        CountDownLatch l2 = new CountDownLatch(1);
        CountDownLatch l3 = new CountDownLatch(1);

        // Block until sequencer operational.
        r.getSequencerView().next();

        // first thread: create and manipulate map
        scheduleConcurrently(t -> {

            // signal start to second thread
            l1.countDown();

            // populate the map
            for (long i = 0; i < (long) PARAMETERS.NUM_ITERATIONS_LOW; i++)
                testTable.insert(i, i);

            assertThat(testTable.get(0L)).isEqualTo(0L);
            assertThat(testTable.size()).isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW);

            // wait for second thread to advance
            l2.await();

            // manipulate the map, verify that keys set is unmodified,
            // the original map is modified
            testTable.delete(0L);

            assertThat(testTable.containsKey(0L)).isFalse();
            assertThat(testTable.size()).isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW - 1);

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
            Set<Long> keys = testTable.keySet();
            int snapshotSize = keys.size();

            if (snapshotSize > 0)
                assertThat(keys.contains(0L)).isTrue();

            // signal that one snapshot was taken already
            l2.countDown();

            // verify that the immutable snapshot remains immutable
            while (l3.getCount() > 0) {
                assertThat(keys.size()).isEqualTo(snapshotSize);
                if (snapshotSize > 0)
                    assertThat(keys.contains(0L)).isTrue();
            }
            l3.await();
            assertThat(keys.size()).isEqualTo(snapshotSize);
            if (snapshotSize > 0)
                assertThat(keys.contains(0L)).isTrue();
        } );

        scheduleConcurrently(t -> {
            l3.await();

            // get a snapshot of the keys;
            Set<Long> keys = testTable.keySet();
            assertThat(keys.size()).isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW-1);
            assertThat(keys.contains(0L)).isFalse();
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_NORMAL);

    }
}
