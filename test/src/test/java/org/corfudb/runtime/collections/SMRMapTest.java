package org.corfudb.runtime.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import lombok.Data;
import lombok.Getter;
import lombok.ToString;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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
    public void canReadWriteToSingle() throws Exception {
        PersistentCorfuTable<String, String> testTable = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        testTable.clear();
        assertThat(testTable.get("a")).isNull();
        testTable.insert("a", "a");
        assertThat(testTable.get("a")).isEqualTo("a");
        testTable.insert("a", "b");
        assertThat(testTable.get("a")).isEqualTo("b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToSinglePrimitive() throws Exception {
        getRuntime().setCacheDisabled(true);
        PersistentCorfuTable<Long, Double> testTable = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setSerializer(Serializers.PRIMITIVE)
                .setTypeToken(new TypeToken<PersistentCorfuTable<Long, Double>>() {})
                .open();

        final double PRIMITIVE_1 = 2.4;
        final double PRIMITIVE_2 = 4.5;

        testTable.clear();
        assertThat(testTable.get(1L)).isNull();
        testTable.insert(1L, PRIMITIVE_1);
        assertThat(testTable.get(1L)).isEqualTo(PRIMITIVE_1);
        testTable.clear();
        assertThat(testTable.get(1L)).isNull();
        testTable.insert(1L, PRIMITIVE_2);
        assertThat(testTable.get(1L)).isEqualTo(PRIMITIVE_2);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canGetID() throws Exception {
        UUID id = UUID.randomUUID();
        ICorfuSMR testTable = getRuntime().getObjectsView()
                .build()
                .setStreamID(id)
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        assertThat(id).isEqualTo(testTable.getCorfuStreamID());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void loadsFollowedByGets() throws Exception {
        PersistentCorfuTable<String, String> testTable = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        testTable.clear();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            testTable.insert(Integer.toString(i), Integer.toString(i));
        }

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            assertThat(testTable.get(Integer.toString(i))).isEqualTo(Integer.toString(i));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canContainOtherCorfuObjects() throws Exception {
        PersistentCorfuTable<String, String> testTable = getRuntime().getObjectsView()
                .build()
                .setStreamName("test 1")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();
        testTable.clear();
        testTable.insert("z", "e");
        PersistentCorfuTable<String, PersistentCorfuTable<String, String>> testTable2 = getRuntime().getObjectsView()
                .build()
                .setStreamName("test 2")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, PersistentCorfuTable<String, String>>>() {})
                .open();
        testTable2.insert("a", testTable);

        assertThat(testTable2.get("a").get("z")).isEqualTo("e");

        testTable2.get("a").insert("y", "f");

        assertThat(testTable.get("y")).isEqualTo("f");

        PersistentCorfuTable<String, String> testTable3 = getRuntime().getObjectsView()
                .build()
                .setStreamName("test 1")
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        assertThat(testTable3.get("y")).isEqualTo("f");
    }

    @Test
    public void loadsFollowedByGetsConcurrent() throws Exception {
        PersistentCorfuTable<String, String> testTable = getRuntime().getObjectsView()
                .build()
                .setStreamID(UUID.randomUUID())
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        final int num_threads = PARAMETERS.CONCURRENCY_SOME;
        final int num_records = PARAMETERS.NUM_ITERATIONS_LOW;
        testTable.clear();

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                testTable.insert(Integer.toString(i), Integer.toString(i));
            }
        });

        long startTime = System.currentTimeMillis();
        executeScheduled(num_threads, PARAMETERS.TIMEOUT_LONG);
        calculateRequestsPerSecond("WPS", num_records * num_threads, startTime);

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testTable.get(Integer.toString(i))).isEqualTo(Integer.toString(i));
            }
        });

        startTime = System.currentTimeMillis();
        executeScheduled(num_threads, PARAMETERS.TIMEOUT_LONG);
        calculateRequestsPerSecond("RPS", num_records * num_threads, startTime);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void loadsFollowedByGetsConcurrentMultiView() throws Exception {
        // Increasing hole fill delay to avoid intermittent AppendExceptions.
        final int longHoleFillRetryLimit = 50;
        r.getParameters().setHoleFillRetry(longHoleFillRetryLimit);

        final int num_threads = 5;
        final int num_records = 1000;

        PersistentCorfuTable<String, String>[] testTables =
                IntStream.range(0, num_threads)
                .mapToObj(i -> getRuntime().getObjectsView()
                        .build()
                        .setStreamID(UUID.randomUUID())
                        .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                        .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {
                        })
                        .option(ObjectOpenOption.NO_CACHE)
                        .open())
                .toArray(PersistentCorfuTable[]::new);

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testTables[threadNumber].get(Integer.toString(i))).isNull();
                testTables[threadNumber].insert(Integer.toString(i), Integer.toString(i));
            }
        });

        long startTime = System.currentTimeMillis();
        executeScheduled(num_threads, PARAMETERS.TIMEOUT_LONG);
        calculateRequestsPerSecond("WPS", num_records * num_threads, startTime);

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testTables[threadNumber].get(Integer.toString(i)))
                        .isEqualTo(Integer.toString(i));
            }
        });

        startTime = System.currentTimeMillis();
        executeScheduled(num_threads, PARAMETERS.TIMEOUT_LONG);
        calculateRequestsPerSecond("RPS", num_records * num_threads, startTime);
    }

    @Test
    @SuppressWarnings("unchecked")
    @Ignore // TODO(Zach):
    public void readSetDiffFromWriteSet() throws Exception {
        PersistentCorfuTable<String, String> testTable = getRuntime().getObjectsView()
                .build()
                .setStreamName("test1")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        PersistentCorfuTable<String, String> testTable2 = getRuntime().getObjectsView()
                .build()
                .setStreamName("test2")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        testTable.insert("a", "b");
        testTable2.insert("a", "c");

        Semaphore s1 = new Semaphore(0);
        Semaphore s2 = new Semaphore(0);
        scheduleConcurrently(1, threadNumber -> {
            s1.tryAcquire(PARAMETERS.TIMEOUT_NORMAL.toMillis(),
                    TimeUnit.MILLISECONDS);
            getRuntime().getObjectsView().TXBegin();
            testTable2.insert("a", "d");
            getRuntime().getObjectsView().TXEnd();
            s2.release();
        });

        scheduleConcurrently(1, threadNumber -> {
            getRuntime().getObjectsView().TXBegin();
            // PersistentCorfuTable does not implement compute
            // testTable.compute("b", (k, v) -> testTable2.get("a"));
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
    public void canUpdateSingleObjectTransactionally() throws Exception {
       PersistentCorfuTable<String, String> testTable = getRuntime().getObjectsView()
               .build()
               .setStreamName("test")
               .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
               .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
               .open();

       getRuntime().getObjectsView().TXBegin();
       assertThat(testTable.get("a")).isNull();
       testTable.insert("a", "a");
       assertThat(testTable.get("a")).isEqualTo("a");
       testTable.delete("a");
       assertThat(testTable.get("a")).isNull();
       testTable.insert("a", "b");
       assertThat(testTable.get("a")).isEqualTo("b");
       getRuntime().getObjectsView().TXEnd();

       assertThat(testTable.get("a")).isEqualTo("b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void multipleTXesAreApplied() throws Exception {
        PersistentCorfuTable<String, String> testTable = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        IntStream.range(0, PARAMETERS.NUM_ITERATIONS_LOW).asLongStream()

                .forEach(l -> {
                    try {
                        assertThat(testTable.size()).isEqualTo(l);
                        getRuntime().getObjectsView().TXBegin();
                        testTable.insert(Long.toString(l), Long.toString(l));
                        assertThat(testTable.size()).isEqualTo(l + 1);
                        getRuntime().getObjectsView().TXEnd();
                        assertThat(testTable.size()).isEqualTo(l + 1);
                    } catch (TransactionAbortedException tae) {
                        throw new RuntimeException(tae);
                    }
                });

        assertThat(testTable.size()).isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void multipleTXesAreAppliedWOAccessors() throws Exception {
        PersistentCorfuTable<String, String> testTable = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        IntStream.range(0, PARAMETERS.NUM_ITERATIONS_LOW).asLongStream()
                .forEach(l -> {
                    try {
                        getRuntime().getObjectsView().TXBegin();
                        testTable.insert(Long.toString(l), Long.toString(l));
                        getRuntime().getObjectsView().TXEnd();
                    } catch (TransactionAbortedException tae) {
                        throw new RuntimeException(tae);
                    }
                });

        assertThat(testTable.size()).isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void mutatorFollowedByATransaction() throws Exception {
        PersistentCorfuTable<String, String> testTable = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        testTable.clear();
        getRuntime().getObjectsView().TXBegin();
        assertThat(testTable.get("a")).isNull();
        testTable.insert("a", "a");
        assertThat(testTable.get("a")).isEqualTo("a");
        testTable.insert("a", "b");
        assertThat(testTable.get("a")).isEqualTo("b");
        getRuntime().getObjectsView().TXEnd();

        assertThat(testTable.get("a")).isEqualTo("b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void objectViewCorrectlyReportsInsideTX() throws Exception {
        assertThat(getRuntime().getObjectsView().TXActive()).isFalse();
        getRuntime().getObjectsView().TXBegin();
        assertThat(getRuntime().getObjectsView().TXActive()).isTrue();
        getRuntime().getObjectsView().TXEnd();
        assertThat(getRuntime().getObjectsView().TXActive()).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canUpdateSingleObjectTransactionallyWhenCached() throws Exception {
        r.setCacheDisabled(false);

        PersistentCorfuTable<String, String> testTable = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        getRuntime().getObjectsView().TXBegin();
        assertThat(testTable.get("a")).isNull();
        testTable.insert("a", "a");
        assertThat(testTable.get("a")).isEqualTo("a");
        testTable.insert("a", "b");
        assertThat(testTable.get("a")).isEqualTo("b");
        getRuntime().getObjectsView().TXEnd();

        assertThat(testTable.get("a")).isEqualTo("b");
    }


    @Test
    @SuppressWarnings("unchecked")
    public void abortedTransactionsCannotBeReadOnSingleObject() throws Exception {
        PersistentCorfuTable<String, String> testTable = getRuntime().getObjectsView()
                .build()
                .setStreamName("test")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        testTable.clear();
        testTable.insert("z", "z");
        assertThat(testTable.size()).isEqualTo(1);

        getRuntime().getObjectsView().TXBegin();
        assertThat(testTable.get("a")).isNull();
        testTable.insert("a", "a");
        assertThat(testTable.get("a")).isEqualTo("a");
        testTable.insert("a", "b");
        assertThat(testTable.get("a")).isEqualTo("b");
        testTable.clear();
        assertThat(testTable.get("a")).isNull();
        getRuntime().getObjectsView().TXAbort();
        assertThat(testTable.size()).isEqualTo(1);
        // TODO(Zach): FAILS
    }

    @Test
    @SuppressWarnings("unchecked")
    public void modificationDuringTransactionCausesAbort() throws Exception {
        PersistentCorfuTable<String, String> testTable = getRuntime().getObjectsView()
                .build()
                .setStreamName("A")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        assertThat(testTable.get("a")).isNull();
        testTable.insert("a", "z");
        getRuntime().getObjectsView().TXBegin();
        assertThat(testTable.get("a")).isEqualTo("z");
        testTable.insert("a", "a");
        assertThat(testTable.get("a")).isEqualTo("a");
        testTable.insert("a", "b");
        assertThat(testTable.get("a")).isEqualTo("b");

        CompletableFuture cf = CompletableFuture.runAsync(() -> {
            PersistentCorfuTable<String, String> testTable2 = getRuntime().getObjectsView()
                    .build()
                    .setStreamName("A")
                    .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                    .setSerializer(Serializers.getDefaultSerializer())
                    .option(ObjectOpenOption.NO_CACHE)
                    .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                    .open();

            getRuntime().getObjectsView().TXBegin();
            testTable2.insert("a", "f");
            getRuntime().getObjectsView().TXEnd();
        });
        cf.join();
        assertThatThrownBy(() -> getRuntime().getObjectsView().TXEnd())
                .isInstanceOf(TransactionAbortedException.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void smrMapCanContainCustomObjects() throws Exception {
        PersistentCorfuTable<String, TestObject> testTable = getRuntime().getObjectsView()
                .build()
                .setStreamName("A")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, TestObject>>() {})
                .open();

        testTable.insert("A", new TestObject("A", 2, ImmutableMap.of("A", "B")));
        assertThat(testTable.get("A").getTestString()).isEqualTo("A");
        assertThat(testTable.get("A").getTestInt()).isEqualTo(2);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void smrMapCanContainCustomObjectsInsideTXes() throws Exception {
        PersistentCorfuTable<String, TestObject> testTable = getRuntime().getObjectsView()
                .build()
                .setStreamName("A")
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, TestObject>>() {})
                .open();

        IntStream.range(0, PARAMETERS.NUM_ITERATIONS_LOW)
                .forEach(l -> {
                    try {
                        getRuntime().getObjectsView().TXBegin();
                        testTable.insert(Integer.toString(l),
                                new TestObject(Integer.toString(l), l,
                                        ImmutableMap.of(Integer.toString(l), l)));
                        if (l > 0) {
                            assertThat(testTable.get(Integer.toString(l - 1)).getTestInt()).isEqualTo(l - 1);
                        }
                        getRuntime().getObjectsView().TXEnd();
                    } catch (TransactionAbortedException tae) {
                        throw new RuntimeException(tae);
                    }
                });

        assertThat(testTable.get("3").getTestString()).isEqualTo("3");
        assertThat(testTable.get("3").getTestInt()).isEqualTo(Integer.parseInt("3"));
    }

    AtomicInteger aborts;

    void getMultiViewSM(int numThreads) {

        UUID mapStream = UUID.randomUUID();
        PersistentCorfuTable<String, String>[] testTables =
                IntStream.range(0, numThreads)
                        .mapToObj(i -> getRuntime().getObjectsView()
                                .build()
                                .setStreamID(mapStream)
                                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {
                                })
                                .option(ObjectOpenOption.NO_CACHE)
                                .open())
                        .toArray(PersistentCorfuTable[]::new);

        // # keys indicate how much contention there will be
        final int numKeys = numThreads * 5;

        Random r = new Random();

        // state 0: start a transaction
        addTestStep((ignored_task_num) -> {
            getRuntime().getObjectsView().TXBegin();
        });

        // state 1: do a put and a get
        addTestStep( (task_num) -> {
            final int putKey = r.nextInt(numKeys);
            final int getKey = r.nextInt(numKeys);

            // TODO(Zach): this creates null values
            testTables[task_num%numThreads].insert(Integer.toString(putKey),
                    testTables[task_num%numThreads].get(Integer.toString(getKey)));

            testTables[task_num%numThreads].get(Integer.toString(putKey)); // Generate conflict information since insert has no upcall
        });

        // state 2 (final): ask to commit the transaction
        addTestStep( (ignored_task_num) -> {
            try {
                getRuntime().getObjectsView().TXEnd();
            } catch (TransactionAbortedException tae) {
                aborts.incrementAndGet();
            }
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void concurrentAbortMultiViewInterleaved() throws Exception {
        final int numThreads = PARAMETERS.CONCURRENCY_SOME;
        final int numRecords = PARAMETERS.NUM_ITERATIONS_LOW;

        long startTime = System.currentTimeMillis();
        aborts = new AtomicInteger();

        getMultiViewSM(numThreads);
        // invoke the interleaving engine
        scheduleInterleaved(numThreads, numThreads*numRecords);

        // print stats..
        calculateRequestsPerSecond("TPS", numRecords * numThreads, startTime);
        calculateAbortRate(aborts.get(), numRecords * numThreads);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void concurrentAbortMultiViewThreaded() throws Exception {
        final int numThreads = PARAMETERS.CONCURRENCY_SOME;
        final int numRecords = PARAMETERS.NUM_ITERATIONS_LOW;

        long startTime = System.currentTimeMillis();
        aborts = new AtomicInteger();

        getMultiViewSM(numThreads);
        // invoke the interleaving engine
        scheduleThreaded(numThreads, numThreads*numRecords);

        // print stats..
        calculateRequestsPerSecond("TPS", numRecords * numThreads, startTime);
        calculateAbortRate(aborts.get(), numRecords * numThreads);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void bulkReads() throws Exception {
        UUID stream = UUID.randomUUID();
        PersistentCorfuTable<String, String> testTable = getRuntime()
                .getObjectsView()
                .build()
                .setStreamID(stream)
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .open();

        testTable.clear();
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            assertThat(testTable.get(Integer.toString(i))).isNull();
            testTable.insert(Integer.toString(i), Integer.toString(i));
        }

        // Do a bulk read of the stream by initializing a new view.
        long startTime = System.nanoTime();
        PersistentCorfuTable<String, String> testTable2 = getRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamID(stream)
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .option(ObjectOpenOption.NO_CACHE)
                .open();
        // Do a get to prompt the sync
        assertThat(testTable2.get(Integer.toString(0))).isEqualTo(Integer.toString(0));
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
