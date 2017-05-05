package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Test;

import java.util.Map;
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
        Map<String, String> testMap = getDefaultRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<FGMap<String, String>>() {})
                .open();

        testMap.clear();
        assertThat(testMap.put("a", "a"))
                .isNull();
        assertThat(testMap.put("a", "b"))
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
        Map<String, String> testMap = getDefaultRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<FGMap<String, String>>() {})
                .open();

        testMap.clear();
        assertThat(testMap)
                .isEmpty();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }

        assertThat(testMap)
                .hasSize(PARAMETERS.NUM_ITERATIONS_LOW);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canNestTX()
            throws Exception {
        Map<String, String> testMap = getDefaultRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<FGMap<String, String>>() {})
                .open();

        testMap.clear();
        assertThat(testMap)
                .isEmpty();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }

        getRuntime().getObjectsView().TXBegin();
        int size = testMap.size();
        testMap.put("size", Integer.toString(size));
        getRuntime().getObjectsView().TXEnd();

        assertThat(testMap.size())
                .isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW + 1);
        assertThat(testMap.get("size"))
                .isEqualTo(Integer.toString(PARAMETERS.NUM_ITERATIONS_LOW));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canClear()
            throws Exception {
        Map<String, String> testMap = getDefaultRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<FGMap<String, String>>() {})
                .open();

        testMap.clear();
        assertThat(testMap)
                .isEmpty();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }

        assertThat(testMap)
                .hasSize(PARAMETERS.NUM_ITERATIONS_LOW);

        testMap.put("a", "b");
        testMap.clear();
        assertThat(testMap)
                .isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canClearInTX()
            throws Exception {
        getDefaultRuntime();

        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<FGMap<String, String>>() {})
                .open();

        testMap.clear();
        assertThat(testMap)
                .isEmpty();

        getRuntime().getObjectsView().TXBegin();
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }
        testMap.clear();
        assertThat(testMap)
                .isEmpty();
//                .hasSize(0);
        getRuntime().getObjectsView().TXEnd();

        assertThat(testMap)
                .hasSize(0);
    }

    /* Test disabled until constructor persistence is enabled
        for compiled objects.
     */
    /*
    @Test
    public void canVaryBucketCount()
            throws Exception {
<<<<<<< HEAD
        FGMap<String, String> testMap = getDefaultRuntime().getObjectsView()
                .build()
                .setArguments(101)
                .setStreamName("test")
                .setTypeToken(new TypeToken<FGMap<String, String>>() {})
                .open();

=======
        FGMap<String, String> testMap = getDefaultRuntime().getObjectsView().open("hello", FGMap.class,
                PARAMETERS.NUM_ITERATIONS_LOW + 1);
>>>>>>> master
        testMap.clear();
        assertThat(testMap)
                .isEmpty();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }

        FGMap<String, String> testMap2 = getRuntime().getObjectsView()
                .build()
                .addOption(ObjectOpenOptions.NO_CACHE)
                .setStreamName("test")
                .setTypeToken(new TypeToken<FGMap<String, String>>() {})
                .open();

        assertThat(testMap2)
                .hasSize(PARAMETERS.NUM_ITERATIONS_LOW);
        assertThat(testMap2.getNumBuckets())
                .isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW + 1);
    }
    */

    @Test
    @SuppressWarnings("unchecked")
    public void loadsFollowedByGetsConcurrent()
            throws Exception {
        getDefaultRuntime();

        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<FGMap<String, String>>() {})
                .open();

        final int num_threads = PARAMETERS.CONCURRENCY_SOME;
        final int num_records = PARAMETERS.NUM_ITERATIONS_LOW;
        testMap.clear();

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                        .isEqualTo(null);
            }
        });

        long startTime = System.currentTimeMillis();
        executeScheduled(num_threads, PARAMETERS.TIMEOUT_LONG);
        calculateRequestsPerSecond("WPS", num_records * num_threads, startTime);

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testMap.get(Integer.toString(i)))
                        .isEqualTo(Integer.toString(i));
            }
        });

        startTime = System.currentTimeMillis();
        executeScheduled(num_threads, PARAMETERS.TIMEOUT_LONG);
        calculateRequestsPerSecond("RPS", num_records * num_threads, startTime);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void abortRateIsLowForSimpleTX()
            throws Exception {
        getDefaultRuntime();

        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<FGMap<String, String>>() {})
                .open();


        final int num_threads = PARAMETERS.CONCURRENCY_SOME;
        final int num_records = PARAMETERS.NUM_ITERATIONS_LOW;
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
        executeScheduled(num_threads, PARAMETERS.TIMEOUT_LONG);
        calculateRequestsPerSecond("TPS", num_records * num_threads, startTime);

        calculateAbortRate(aborts.get(), num_records * num_threads);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void checkClearCalls()
            throws Exception {
        getDefaultRuntime();

        final int NUM_BUCKETS = 20;
        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setArguments(NUM_BUCKETS)
                .setStreamName("test")
                .setTypeToken(new TypeToken<FGMap<String, String>>() {})
                .open();

        final int num_threads = PARAMETERS.CONCURRENCY_SOME;
        final int num_records = PARAMETERS.NUM_ITERATIONS_LOW;

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                testMap.clear();
                assertThat(testMap)
                        .isEmpty();
            }
        });

        long startTime = System.currentTimeMillis();
        executeScheduled(num_threads, PARAMETERS.TIMEOUT_LONG);
        calculateRequestsPerSecond("OPS", num_records * num_threads, startTime);
    }
}
