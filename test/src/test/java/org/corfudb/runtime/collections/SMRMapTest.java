package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuObject;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRObject;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.ObjectOpenOptions;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 1/7/16.
 */
public class SMRMapTest extends AbstractViewTest {
    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    public CorfuRuntime r;


    @Before
    public void setRuntime() throws Exception {
        r = getDefaultRuntime().connect();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToSingle()
            throws Exception {
        Map<String, String> testMap = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        testMap.clear();
        assertThat(testMap.put("a", "a"))
                .isNull();
        assertThat(testMap.put("a", "b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canGetID()
            throws Exception {
        UUID id = UUID.randomUUID();
        ICorfuSMR testMap = (ICorfuSMR) getRuntime().getObjectsView()
                .build()
                .setStreamID(id)
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        assertThat(id)
                .isEqualTo(testMap.getCorfuStreamID());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void loadsFollowedByGets()
            throws Exception {
        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        testMap.clear();
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                    .isNull();
        }
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            assertThat(testMap.get(Integer.toString(i)))
                    .isEqualTo(Integer.toString(i));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canContainOtherCorfuObjects()
            throws Exception {
        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test 1")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();
        testMap.clear();
        testMap.put("z", "e");
        Map<String, Map<String, String>> testMap2 = getRuntime().getObjectsView()
                .build()
                .setStreamName("test 2")
                .setTypeToken(new TypeToken<SMRMap<String, Map<String, String>>>() {})
                .open();
        testMap2.put("a", testMap);

        assertThat(testMap2.get("a").get("z"))
                .isEqualTo("e");

        testMap2.get("a").put("y", "f");

        assertThat(testMap.get("y"))
                .isEqualTo("f");

        Map<String, String> testMap3 = getRuntime().getObjectsView()
                .build()
                .setStreamName("test 1")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        assertThat(testMap3.get("y"))
                .isEqualTo("f");

    }

    @Test
    @SuppressWarnings("unchecked")
    public void canContainNullObjects()
            throws Exception {
        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("a")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        testMap.clear();
        testMap.put("z", null);
        assertThat(testMap.get("z"))
                .isEqualTo(null);
        Map<String, String> testMap2 = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("a")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        assertThat(testMap2.get("z"))
                .isEqualTo(null);
    }

    @Test
    public void loadsFollowedByGetsConcurrent()
            throws Exception {
        r.setBackpointersDisabled(true);

        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamID(UUID.randomUUID())
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
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
    public void loadsFollowedByGetsConcurrentMultiView()
            throws Exception {
        r.setBackpointersDisabled(true);

        final int num_threads = 5;
        final int num_records = 1000;

        Map<String, String>[] testMap =
                IntStream.range(0, num_threads)
                .mapToObj(i -> {
                    return getRuntime().getObjectsView()
                            .build()
                            .setStreamID(UUID.randomUUID())
                            .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                            })
                            .addOption(ObjectOpenOptions.NO_CACHE)
                            .open();
                })
                .toArray(Map[]::new);

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testMap[threadNumber].put(Integer.toString(i), Integer.toString(i)))
                        .isEqualTo(null);
            }
        });

        long startTime = System.currentTimeMillis();
        executeScheduled(num_threads, PARAMETERS.TIMEOUT_LONG);
        calculateRequestsPerSecond("WPS", num_records * num_threads, startTime);

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testMap[threadNumber].get(Integer.toString(i)))
                        .isEqualTo(Integer.toString(i));
            }
        });

        startTime = System.currentTimeMillis();
        executeScheduled(num_threads, PARAMETERS.TIMEOUT_LONG);
        calculateRequestsPerSecond("RPS", num_records * num_threads, startTime);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void collectionsStreamInterface()
            throws Exception {
        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        testMap.put("a", "b");
        getRuntime().getObjectsView().TXBegin();
        if (testMap.values().stream().anyMatch(x -> x.equals("c"))) {
            throw new Exception("test");
        }
        testMap.compute("b",
                (k, v) -> "c");
        getRuntime().getObjectsView().TXEnd();
        assertThat(testMap)
                .containsEntry("b", "c");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void readSetDiffFromWriteSet()
            throws Exception {
        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test1")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        Map<String, String> testMap2 = getRuntime().getObjectsView()
                .build()
                .setStreamName("test2")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        testMap.put("a", "b");
        testMap2.put("a", "c");

        Semaphore s1 = new Semaphore(0);
        Semaphore s2 = new Semaphore(0);
        scheduleConcurrently(1, threadNumber -> {
            s1.tryAcquire(PARAMETERS.TIMEOUT_NORMAL.toMillis(),
                    TimeUnit.MILLISECONDS);
            testMap2.put("c", "d");
            s2.release();
        });

        scheduleConcurrently(1, threadNumber -> {
            getRuntime().getObjectsView().TXBegin();
            testMap.compute("b", (k, v) -> testMap2.get("a"));
            s1.release();
            s2.tryAcquire(PARAMETERS.TIMEOUT_NORMAL.toMillis(),
                    TimeUnit.MILLISECONDS);
            assertThatThrownBy(() -> getRuntime().getObjectsView().TXEnd())
                    .isInstanceOf(TransactionAbortedException.class);
        });
        executeScheduled(PARAMETERS.CONCURRENCY_TWO, PARAMETERS.TIMEOUT_NORMAL);
    }

   @Test
    @SuppressWarnings("unchecked")
    public void canUpdateSingleObjectTransacationally()
            throws Exception {
        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        getRuntime().getObjectsView().TXBegin();
        assertThat(testMap.put("a", "a"))
                .isNull();
        assertThat(testMap.put("a", "b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");
        getRuntime().getObjectsView().TXEnd();
        assertThat(testMap.get("a"))
                .isEqualTo("b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void multipleTXesAreApplied()
            throws Exception {

        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        IntStream.range(0, PARAMETERS.NUM_ITERATIONS_LOW).asLongStream()

                .forEach(l -> {
                    try {
                        assertThat(testMap)
                                .hasSize((int) l);
                        getRuntime().getObjectsView().TXBegin();
                        assertThat(testMap.put(Long.toString(l), Long.toString(l)))
                                .isNull();
                        assertThat(testMap)
                                .hasSize((int) l + 1);
                        getRuntime().getObjectsView().TXEnd();
                        assertThat(testMap)
                                .hasSize((int) l + 1);
                    } catch (TransactionAbortedException tae) {
                        throw new RuntimeException(tae);
                    }
                });

        assertThat(testMap)
                .hasSize(PARAMETERS.NUM_ITERATIONS_LOW);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void multipleTXesAreAppliedWOAccessors()
            throws Exception {

        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        IntStream.range(0, PARAMETERS.NUM_ITERATIONS_LOW).asLongStream()
                .forEach(l -> {
                    try {
                        getRuntime().getObjectsView().TXBegin();
                        assertThat(testMap.put(Long.toString(l), Long.toString(l)))
                                .isNull();
                        getRuntime().getObjectsView().TXEnd();
                    } catch (TransactionAbortedException tae) {
                        throw new RuntimeException(tae);
                    }
                });

        assertThat(testMap)
                .hasSize(PARAMETERS.NUM_ITERATIONS_LOW);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void mutatorFollowedByATransaction()
            throws Exception {
        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        testMap.clear();
        getRuntime().getObjectsView().TXBegin();
        assertThat(testMap.put("a", "a"))
                .isNull();
        assertThat(testMap.put("a", "b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");
        getRuntime().getObjectsView().TXEnd();
        assertThat(testMap.get("a"))
                .isEqualTo("b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void objectViewCorrectlyReportsInsideTX()
            throws Exception {
        assertThat(getRuntime().getObjectsView().TXActive())
                .isFalse();
        getRuntime().getObjectsView().TXBegin();
        assertThat(getRuntime().getObjectsView().TXActive())
                .isTrue();
        getRuntime().getObjectsView().TXEnd();
        assertThat(getRuntime().getObjectsView().TXActive())
                .isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canUpdateSingleObjectTransacationallyWhenCached()
            throws Exception {
        r.setCacheDisabled(false);

        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        getRuntime().getObjectsView().TXBegin();
        assertThat(testMap.put("a", "a"))
                .isNull();
        assertThat(testMap.put("a", "b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");
        getRuntime().getObjectsView().TXEnd();
        assertThat(testMap.get("a"))
                .isEqualTo("b");
    }


    @Test
    @SuppressWarnings("unchecked")
    public void abortedTransactionsCannotBeReadOnSingleObject()
            throws Exception {
        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        getRuntime().getObjectsView().TXBegin();
        testMap.clear();
        assertThat(testMap.put("a", "a"))
                .isNull();
        assertThat(testMap.put("a", "b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");
        getRuntime().getObjectsView().TXAbort();
        assertThat(testMap.size())
                .isEqualTo(0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void modificationDuringTransactionCausesAbort()
            throws Exception {
        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("A")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        assertThat(testMap.put("a", "z"));
        getRuntime().getObjectsView().TXBegin();
        assertThat(testMap.put("a", "a"))
                .isEqualTo("z");
        assertThat(testMap.put("a", "b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");
        CompletableFuture cf = CompletableFuture.runAsync(() -> {
            Map<String, String> testMap2 = getRuntime().getObjectsView()
                    .build()
                    .setStreamName("A")
                    .setSerializer(Serializers.JSON)
                    .addOption(ObjectOpenOptions.NO_CACHE)
                    .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                    .open();

            testMap2.put("a", "f");
        });
        cf.join();
        assertThatThrownBy(() -> getRuntime().getObjectsView().TXEnd())
                .isInstanceOf(TransactionAbortedException.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void smrMapCanContainCustomObjects()
            throws Exception {
        Map<String, TestObject> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("A")
                .setTypeToken(new TypeToken<SMRMap<String, TestObject>>() {})
                .open();

        testMap.put("A", new TestObject("A", 2, ImmutableMap.of("A", "B")));
        assertThat(testMap.get("A").getTestString())
                .isEqualTo("A");
        assertThat(testMap.get("A").getTestInt())
                .isEqualTo(2);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void smrMapCanContainCustomObjectsInsideTXes()
            throws Exception {
        Map<String, TestObject> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("A")
                .setTypeToken(new TypeToken<SMRMap<String, TestObject>>() {})
                .open();

        IntStream.range(0, PARAMETERS.NUM_ITERATIONS_LOW)
                .forEach(l -> {
                    try {
                        getRuntime().getObjectsView().TXBegin();
                        testMap.put(Integer.toString(l),
                                new TestObject(Integer.toString(l), l,
                                        ImmutableMap.of(
                                Integer.toString(l), l)));
                        if (l > 0) {
                            assertThat(testMap.get(Integer.toString(l - 1)).getTestInt())
                                    .isEqualTo(l - 1);
                        }
                        getRuntime().getObjectsView().TXEnd();
                    } catch (TransactionAbortedException tae) {
                        throw new RuntimeException(tae);
                    }
                });

        assertThat(testMap.get("3").getTestString())
                .isEqualTo("3");
        assertThat(testMap.get("3").getTestInt())
                .isEqualTo(Integer.parseInt("3"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void unusedMutatorAccessor()
            throws Exception {
        Map<String, String> testMap = getRuntime().getObjectsView()
                .build()
                .setStreamName("A")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        testMap.put("a", "z");
    }

    AtomicInteger aborts;

    ArrayList<IntConsumer> getAbortTestSM() {
        Map<String, String> testMap = getRuntime().getObjectsView().build()
                .setStreamID(UUID.randomUUID())
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .addOption(ObjectOpenOptions.NO_CACHE)
                .open();
        AtomicInteger aborts = new AtomicInteger();
        testMap.clear();

        ArrayList<IntConsumer> stateMachine = new ArrayList<IntConsumer>(){

            {
                // state 0: start a transaction
                add ((ignored_task_num) -> {
                    getRuntime().getObjectsView().TXBegin();
                });

                // state 1: do a put
                add( (task_num) -> {
                    assertThat(testMap.put(Integer.toString(task_num),
                            Integer.toString(task_num)))
                            .isEqualTo(null);
                });

                // state 2 (final): ask to commit the transaction
                add ( (ignored_task_num) -> {
                    try {
                        getRuntime().getObjectsView().TXEnd();
                    } catch (TransactionAbortedException tae) {
                        aborts.incrementAndGet();
                    }
                });

            }
        } ;
        return stateMachine;
    }

    @Test
    public void concurrentAbortTestThreaded()
            throws Exception
    {
        final int numThreads =  PARAMETERS.CONCURRENCY_SOME;
        final int numRecords = PARAMETERS.NUM_ITERATIONS_LOW;

        long startTime = System.currentTimeMillis();

        aborts = new AtomicInteger();
        // invoke the threaded engine
        scheduleThreaded(numThreads, numThreads*numRecords, getAbortTestSM());

        // print stats..
        calculateRequestsPerSecond("TPS", numRecords * numThreads, startTime);
        calculateAbortRate(aborts.get(), numRecords * numThreads);

    }

    @Test
    public void concurrentAbortTestInterleaved()
            throws Exception
    {
        final int numThreads =  PARAMETERS.CONCURRENCY_SOME;
        final int numRecords = PARAMETERS.NUM_ITERATIONS_LOW;

        long startTime = System.currentTimeMillis();

        aborts = new AtomicInteger();
        // invoke the interleaving engine
        scheduleInterleaved(numThreads, numThreads*numRecords, getAbortTestSM());

        // print stats..
        calculateRequestsPerSecond("TPS", numRecords * numThreads, startTime);
        calculateAbortRate(aborts.get(), numRecords * numThreads);

    }

    ArrayList<IntConsumer> getMultiViewSM(int numThreads) {

        UUID mapStream = UUID.randomUUID();
        Map<String, String>[] testMap =
                IntStream.range(0, numThreads)
                        .mapToObj(i -> {
                            return getRuntime().getObjectsView()
                                    .build()
                                    .setStreamID(mapStream)
                                    .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                                    })
                                    .addOption(ObjectOpenOptions.NO_CACHE)
                                    .open();
                        })
                        .toArray(Map[]::new);
        AtomicInteger aborts = new AtomicInteger();

        // # keys indicate how much contention there will be
        final int numKeys = numThreads * 5;

        Random r = new Random();

        ArrayList<IntConsumer> stateMachine = new ArrayList<IntConsumer>(){

            {
                // state 0: start a transaction
                add ((ignored_task_num) -> {
                    getRuntime().getObjectsView().TXBegin();
                });

                // state 1: do a put and a get
                add( (task_num) -> {
                    final int putKey = r.nextInt(numKeys);
                    final int getKey = r.nextInt(numKeys);
                    testMap[task_num%numThreads].put(Integer.toString(putKey),
                            testMap[task_num%numThreads].get(Integer.toString(getKey)));
                });

                // state 2 (final): ask to commit the transaction
                add ( (ignored_task_num) -> {
                    try {
                        getRuntime().getObjectsView().TXEnd();
                    } catch (TransactionAbortedException tae) {
                        aborts.incrementAndGet();
                    }
                });

            }
        } ;

        return stateMachine;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void concurrentAbortMultiViewInterleaved()
            throws Exception {
        final int numThreads = PARAMETERS.CONCURRENCY_SOME;
        final int numRecords = PARAMETERS.NUM_ITERATIONS_LOW;
        AtomicInteger aborts = new AtomicInteger();


        long startTime = System.currentTimeMillis();

        // invoke the interleaving engine
        scheduleInterleaved(numThreads, numThreads*numRecords,
                getMultiViewSM(numThreads)
        );

        // print stats..
        calculateRequestsPerSecond("TPS", numRecords * numThreads, startTime);
        calculateAbortRate(aborts.get(), numRecords * numThreads);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void concurrentAbortMultiViewThreaded()
            throws Exception {
        final int numThreads = PARAMETERS.CONCURRENCY_SOME;
        final int numRecords = PARAMETERS.NUM_ITERATIONS_LOW;
        AtomicInteger aborts = new AtomicInteger();

        long startTime = System.currentTimeMillis();

        // invoke the interleaving engine
        scheduleThreaded(numThreads, numThreads*numRecords,
                getMultiViewSM(numThreads)
        );

        // print stats..
        calculateRequestsPerSecond("TPS", numRecords * numThreads, startTime);
        calculateAbortRate(aborts.get(), numRecords * numThreads);
    }
    @Test
    @SuppressWarnings("unchecked")
    public void bulkReads()
            throws Exception {
        UUID stream = UUID.randomUUID();
        Map<String, String> testMap = getRuntime()
                .getObjectsView()
                .build()
                .setStreamID(stream)
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        testMap.clear();
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                    .isNull();
        }

        // Do a bulk read of the stream by initializing a new view.
        final int num_threads = 1;

        long startTime = System.nanoTime();
        Map<String, String> testMap2 = getRuntime().getObjectsView().build()
                .setType(SMRMap.class)
                .setStreamID(stream)
                .addOption(ObjectOpenOptions.NO_CACHE)
                .open();
        // Do a get to prompt the sync
        assertThat(testMap2.get(Integer.toString(0)))
                .isEqualTo(Integer.toString(0));
        long endTime = System.nanoTime();

        final int MILLISECONDS_TO_MICROSECONDS = 1000;
        testStatus += "Time to sync whole stream=" + String.format("%d us",
                (endTime - startTime) / MILLISECONDS_TO_MICROSECONDS);
    }

    @Data
    @ToString
    static class TestObject {
        final String testString;
        final int testInt;
        final Map<String, Object> deepMap;
    }
}
