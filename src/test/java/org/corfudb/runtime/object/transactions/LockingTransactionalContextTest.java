package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectOpenOptions;
import org.corfudb.runtime.view.TransactionStrategy;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 9/19/16.
 */
public class LockingTransactionalContextTest extends AbstractViewTest {

    @Test
    @SuppressWarnings("unchecked")
    public void simpleLockingTxn() {
        getDefaultRuntime().connect();
        Map<Integer, Integer> map1 = getRuntime()
                                    .getObjectsView().build()
                                    .setStreamName("map-a")
                                    .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {})
                                    .open();

        Map<Integer, Integer> map2 = getRuntime()
                .getObjectsView().build()
                .setStreamName("map-b")
                .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {})
                .open();


        map1.put(0, 100);
        map2.put(100, 0);

        getRuntime().getObjectsView().TXBegin(TransactionStrategy.LOCKING, map1, map2);
        int temp = map2.get(100);
        map2.put(100, map1.get(0));
        map1.put(0, temp);
        getRuntime().getObjectsView().TXEnd();

        assertThat(map1)
                .containsEntry(0, 0);
        map2.get(100);
        assertThat(map2)
                .containsEntry(100, 100);
    }

    // Note:: this is for TESTING only, abort rate will be high because of locks inside maps shared by threads.
    @Test
    @SuppressWarnings("unchecked")
    public void concurrentLockingTxnSwaps()
    throws Exception {
        getDefaultRuntime().connect();

        // initial maps
        Map<Integer, Integer> map1 = getRuntime()
                .getObjectsView().build()
                .setStreamName("map-a")
                .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {})
                .open();

        Map<Integer, Integer> map2 = getRuntime()
                .getObjectsView().build()
                .setStreamName("map-b")
                .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {})
                .open();


        final int num_threads = 10;
        final int num_records = 500;

        IntStream.range(0, num_threads * num_records)
                .forEach(x -> {
                    map1.put(x, 1);
                    map2.put(x, 2);
                });


        AtomicInteger aborts = new AtomicInteger();
        scheduleConcurrently(num_threads, threadNumber -> {

            Map<Integer, Integer> localMap1 = getRuntime()
                    .getObjectsView().build()
                    .setStreamName("map-a")
                    .addOption(ObjectOpenOptions.NO_CACHE)
                    .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {})
                    .open();

            Map<Integer, Integer> localMap2 = getRuntime()
                    .getObjectsView().build()
                    .setStreamName("map-b")
                    .addOption(ObjectOpenOptions.NO_CACHE)
                    .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {})
                    .open();

            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                try {
                    getRuntime().getObjectsView().TXBegin(TransactionStrategy.LOCKING, localMap1, localMap2);
                    int temp = localMap2.get(i);
                    localMap2.put(i, localMap1.get(i));
                    localMap1.put(i, temp);
                    getRuntime().getObjectsView().TXEnd();
                } catch (TransactionAbortedException tae) {
                    aborts.incrementAndGet();
                }
            }
        });

        long startTime = System.currentTimeMillis();
        executeScheduled(num_threads, 30, TimeUnit.SECONDS);
        calculateRequestsPerSecond("TPS", num_records * num_threads, startTime);
        calculateAbortRate(aborts.get(), num_records * num_threads);
    }
}
