package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.ObjectOpenOptions;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.junit.Before;
import org.junit.Test;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

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

        // get a view of the keys
        Set<Long> keys = testMap.keySet();
        assertThat(keys.size())
                .isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW);
        assertThat(keys.contains(0L))
                .isTrue();

        // manipulate the map, verify that keys set is not modified,
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
        CountDownLatch l4 = new CountDownLatch(1);

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
            Set<Long> copy = ImmutableSet.copyOf(keys);
            assertThat(copy)
                    .hasSameSizeAs(keys);

            // signal that one snapshot was taken already
            l2.countDown();

            l3.await();
            //make sure that the contents are the same
            assertThat(keys)
                    .hasSameElementsAs(copy);

        } );

        scheduleConcurrently(t -> {
            l3.await();

            // get a snapshot of the keys;
            Set<Long> keys = testMap.keySet();
            assertThat(keys.size())
                    .isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW-1);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_SHORT);

    }


    @Test
    public void concurrentModificationDoesNotThrowException() {
        Map<String, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .setStreamName("test")
                .open();

        // Add some elements to the map
        t1(() -> map.put("a", "a"));
        t1(() -> map.put("b", "b"));
        t1(() -> map.put("c", "c"));
        t1(() -> map.put("d", "d"));
        t1(() -> map.put("e", "e"));
        t1(() -> map.put("f", "f"));

        // Obtain a keyset and it's iterator
        final AtomicReference<Set<String>> keySet =
                new AtomicReference<>();
        final AtomicReference<Iterator<String>> keyIterator =
                new AtomicReference<>();
        t1(() -> keySet.set(map.keySet()));
        t1(() -> keyIterator.set(keySet.get().iterator()));

        // Begin partial iteration.
        t1(() -> keyIterator.get().next())
                .assertResult()
                .isEqualTo("a");
        t1(() -> keyIterator.get().next())
                .assertResult()
                .isEqualTo("b");

        // On another thread, delete some elements
        t2(() -> map.remove("a"));
        t2(() -> map.remove("d"));
        t2(() -> map.remove("f"));

        // Now continue iteration
        t1(() -> keyIterator.get().next())
                .assertResult()
                .isEqualTo("c");

        // and continue again to deleted item
        t1(() -> keyIterator.get().next())
                .assertResult()
                .isEqualTo("d");
    }

}
