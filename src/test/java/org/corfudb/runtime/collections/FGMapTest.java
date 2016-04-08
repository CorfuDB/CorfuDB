package org.corfudb.runtime.collections;

import lombok.Getter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.runtime.exceptions.ObjectExistsException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectOpenOptions;
import org.junit.Test;

import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    /*
    @Test
    @SuppressWarnings("unchecked")
    public void createOnlyThrowsException()
            throws Exception {
        Map<String,String> testMap = getDefaultRuntime().getObjectsView().build()
                                                        .setType(FGMap.class)
                                                        .setStreamName("map")
                                                        .addOption(ObjectOpenOptions.CREATE_ONLY)
                                                        .open();
        assertThatThrownBy(() -> getDefaultRuntime().getObjectsView().build()
                                                        .setType(FGMap.class)
                                                        .setStreamName("map")
                                                        .addOption(ObjectOpenOptions.CREATE_ONLY)
                                                        .open())
                .isInstanceOf(ObjectExistsException.class);

    }
    */

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
    public void canClear()
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

        testMap.put("a" , "b");
        testMap.clear();
        assertThat(testMap)
                .isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canClearInTX()
            throws Exception {
        Map<String,String> testMap = getDefaultRuntime().getObjectsView().open(UUID.randomUUID(), FGMap.class);
        testMap.clear();
        assertThat(testMap)
                .isEmpty();

        getRuntime().getObjectsView().TXBegin();
        for (int i = 0; i < 100; i++)
        {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }
        testMap.clear();
        assertThat(testMap)
                .isEmpty();
        getRuntime().getObjectsView().TXEnd();

        assertThat(testMap)
                .hasSize(0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canVaryBucketCount()
            throws Exception {
        FGMap<String,String> testMap = getDefaultRuntime().getObjectsView().open("hello", FGMap.class, 101);
        testMap.clear();
        assertThat(testMap)
                .isEmpty();

        for (int i = 0; i < 100; i++)
        {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }

        FGMap<String,String> testMap2 = getRuntime().getObjectsView().open("hello", FGMap.class,
                EnumSet.of(ObjectOpenOptions.NO_CACHE));
        assertThat(testMap2)
                .hasSize(100);
        assertThat(testMap2.getNumBuckets())
                .isEqualTo(101);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void loadsFollowedByGetsConcurrent()
            throws Exception {
        getDefaultRuntime();

        Map<String,String> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(), FGMap.class);

        final int num_threads = 5;
        final int num_records = 500;
        testMap.clear();

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                        .isEqualTo(null);
            }
        });

        long startTime = System.currentTimeMillis();
        executeScheduled(num_threads, 30, TimeUnit.SECONDS);
        calculateRequestsPerSecond("WPS", num_records * num_threads, startTime);

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testMap.get(Integer.toString(i)))
                        .isEqualTo(Integer.toString(i));
            }
        });

        startTime = System.currentTimeMillis();
        executeScheduled(num_threads, 30, TimeUnit.SECONDS);
        calculateRequestsPerSecond("RPS", num_records * num_threads, startTime);
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

        long startTime = System.currentTimeMillis();
        executeScheduled(num_threads, 30, TimeUnit.SECONDS);
        calculateRequestsPerSecond("TPS", num_records * num_threads, startTime);

        calculateAbortRate(aborts.get(), num_records*num_threads);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void checkClearCalls()
            throws Exception {
        getDefaultRuntime();

        Map<String,String> testMap = getRuntime().getObjectsView()
                .open(UUID.randomUUID(), FGMap.class, 20);

        final int num_threads = 5;
        final int num_records = 100;

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                testMap.clear();
                assertThat(testMap)
                        .isEmpty();
            }
        });

        long startTime = System.currentTimeMillis();
        executeScheduled(num_threads, 30, TimeUnit.SECONDS);
        calculateRequestsPerSecond("OPS", num_records * num_threads, startTime);
    }
}
