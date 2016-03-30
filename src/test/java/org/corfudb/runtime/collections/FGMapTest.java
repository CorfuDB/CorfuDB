package org.corfudb.runtime.collections;

import lombok.Getter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 3/29/16.
 */
public class FGMapTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToSingle()
            throws Exception {
        Map<String,String> testMap = getDefaultRuntime().getObjectsView().open(UUID.randomUUID(), FGMap.class);
        testMap.clear();
        assertThat(testMap.put("a","a"))
                .isNull();
        assertThat(testMap.put("a","b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void sizeIsCorrect()
            throws Exception {
        Map<String,String> testMap = getDefaultRuntime().getObjectsView().open(UUID.randomUUID(), FGMap.class);
        testMap.clear();
        assertThat(testMap)
                .isEmpty();

        for (int i = 0; i < 100; i++)
        {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }

        assertThat(testMap)
                .hasSize(100);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canNestTX()
            throws Exception {
        Map<String,String> testMap = getDefaultRuntime().getObjectsView().open(UUID.randomUUID(), FGMap.class);
        testMap.clear();
        assertThat(testMap)
                .isEmpty();

        for (int i = 0; i < 100; i++)
        {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }

        getRuntime().getObjectsView().TXBegin();
        int size = testMap.size();
        testMap.put("size", Integer.toString(size));
        getRuntime().getObjectsView().TXEnd();

        assertThat(testMap.size())
                .isEqualTo(101);
        assertThat(testMap.get("size"))
                .isEqualTo("100");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void loadsFollowedByGetsConcurrent()
            throws Exception {
        getDefaultRuntime();

        Map<String,String> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(), FGMap.class);

        final int num_threads = 5;
        final int num_records = 20;
        testMap.clear();

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                        .isEqualTo(null);
            }
        });
        executeScheduled(num_threads, 30, TimeUnit.SECONDS);

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testMap.get(Integer.toString(i)))
                        .isEqualTo(Integer.toString(i));
            }
        });
        executeScheduled(num_threads, 30, TimeUnit.SECONDS);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void abortRateIsLowForSimpleTX()
            throws Exception {
        getDefaultRuntime();

        Map<String,String> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(), FGMap.class);

        final int num_threads = 5;
        final int num_records = 100;
        AtomicInteger aborts = new AtomicInteger();
        testMap.clear();

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                try {
                    getRuntime().getObjectsView().TXBegin();
                    assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                            .isEqualTo(null);
                    getRuntime().getObjectsView().TXEnd();
                } catch (TransactionAbortedException tae) {
                    aborts.incrementAndGet();
                }
            }
        });
        executeScheduled(num_threads, 30, TimeUnit.SECONDS);

        calculateAbortRate(aborts.get(), num_records*num_threads);
    }
}
