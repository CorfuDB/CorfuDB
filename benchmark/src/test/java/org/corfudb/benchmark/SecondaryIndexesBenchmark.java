package org.corfudb.benchmark;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuOptions.PersistenceOptions;
import org.corfudb.runtime.collections.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SecondaryIndexesBenchmark extends UniverseHook {

    @Param({"false", "true"})
    boolean diskBacked;

    private static final int payloadSize = 1024 * 10; // 10 KB
    private static final int maxPutCount = 1024;
    private static final int maxPutCountPrime = 256;
    private static final int batchSize = 128;

    private static final int minThreadCount = 16;
    private static final int maxThreadCount = 16;

    @State(Scope.Benchmark)
    public static class BenchmarkState extends UniverseBenchmarkState {
        final Map<Schema.Uuid, Schema.StringValueIndex> primeUuids = new HashMap<>();
        final AtomicInteger threadCount = new AtomicInteger(0);
        Table<Schema.Uuid, Schema.StringValueIndex, ?> table;
        CorfuStore store;
        CyclicBarrier latch = new CyclicBarrier(maxThreadCount);
        int iterationCount = 0;
    }

    public static void main(String[] args) throws RunnerException {
        for (int threadCount = minThreadCount; threadCount <= maxThreadCount; threadCount = threadCount * 2) {
            Options opt = jmhBuilder()
                    .threads(threadCount)
                    .addProfiler(AuxReport.class)
                    .build();

            Collection<RunResult> results = new Runner(opt).run();
        }
    }

    private Table<Schema.Uuid, Schema.StringValueIndex, ?> openTable(
            BenchmarkState state) throws
            InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        TableOptions.TableOptionsBuilder optionsBuilder = TableOptions.builder();

        if (diskBacked) {
            final String diskBackedDirectory = "/tmp/";
            final Path persistedCacheLocation = Paths.get(diskBackedDirectory,
                    DEFAULT_STREAM_NAME + state.iterationCount++);
            PersistenceOptions persistenceOptions = PersistenceOptions.newBuilder()
                    .setConsistencyModel(CorfuOptions.ConsistencyModel.READ_COMMITTED)
                    .setDataPath(persistedCacheLocation.toAbsolutePath().toString())
                    .build();
            optionsBuilder.persistenceOptions(persistenceOptions);
        }

        return state.store.openTable(
                DEFAULT_STREAM_NAMESPACE,
                DEFAULT_STREAM_NAME,
                Schema.Uuid.class,
                Schema.StringValueIndex.class,
                null,
                TableOptions.fromProtoSchema(Schema.StringValueIndex.class, optionsBuilder.build())
                );
    }

    @Setup(Level.Iteration)
    public void prepare(BenchmarkState state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        state.threadCount.set(0);
        state.latch.reset();
    }

    @TearDown
    public void tearDown(BenchmarkState state) {
        if (state.table.corfuTable instanceof PersistedCorfuTable) {
            ((PersistedCorfuTable) state.table.corfuTable).publishStats(System.err::println);
        }
        state.corfuClient.shutdown();
        state.wf.getUniverse().shutdown();

    }

    @Setup
    public void setup(BenchmarkState state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        setupUniverseFramework(state);

        state.store = new CorfuStore(state.corfuCluster.getLocalCorfuClient().getRuntime());
        state.table = openTable(state);

        for (int threadId = 1; threadId <= maxThreadCount; threadId++) {
            long putCountPrime = 0;
            try (TxnContext tx = state.store.txn(DEFAULT_STREAM_NAMESPACE)) {
                for (int batchCount = 0; batchCount < maxPutCountPrime / batchSize; batchCount++) {
                    for (int putCount = 0; putCount < batchSize; putCount++) {
                        Schema.Uuid key = Schema.Uuid.newBuilder()
                                .setLsb(-(batchCount * batchSize + putCount))
                                .setMsb(-threadId)
                                .build();
                        final String payload = RandomStringUtils.random(payloadSize);
                        Schema.StringValueIndex value = Schema.StringValueIndex.newBuilder()
                                .setValue(payload)
                                .setSecondary(String.valueOf(-threadId))
                                .build();
                        state.primeUuids.put(key, value);
                        tx.getRecord(state.table, key);
                        tx.putRecord(state.table, key, value, null);
                        putCountPrime++;
                    }
                }
                tx.commit();
            }
            assertThat(putCountPrime).isEqualTo(maxPutCountPrime);
        }
        assertThat(state.primeUuids.size()).isEqualTo(maxPutCountPrime * maxThreadCount);
    }

    @Benchmark
    @Measurement(iterations = 2)
    @Warmup(iterations = 1)
    public void singleThreadedWrite(BenchmarkState state, BenchmarkParams params) throws BrokenBarrierException, InterruptedException {
        long threadId = state.threadCount.incrementAndGet();
        assertThat(threadId)
                .isGreaterThanOrEqualTo(1)
                .isLessThanOrEqualTo(maxThreadCount);

        for (int batchCount = 1; batchCount <= maxPutCount / batchSize; batchCount++) {
            try (TxnContext tx = state.store.txn(DEFAULT_STREAM_NAMESPACE)) {
                for (int putCount = 0; putCount < batchSize; putCount++) {
                    Schema.Uuid key = Schema.Uuid.newBuilder()
                            .setLsb(batchCount * batchSize + putCount)
                            .setMsb(threadId)
                            .build();
                    final String payload = RandomStringUtils.random(payloadSize);
                    Schema.StringValueIndex value = Schema.StringValueIndex.newBuilder()
                            .setValue(payload)
                            .setSecondary(String.valueOf(threadId))
                            .build();
                    tx.getRecord(state.table, key);
                    tx.putRecord(state.table, key, value, null);
                }

                tx.commit();
            }
        }

        final StopWatch watch = new StopWatch();

        //state.latch.await();

        watch.reset();
        watch.start();

        try (TxnContext tx = state.store.txn(DEFAULT_STREAM_NAMESPACE)) {
            assertThat(tx.getByIndex(state.table, "secondary", String.valueOf(threadId)).size()).isEqualTo(maxPutCount);
            tx.txAbort();
        }


        watch.stop();
        AuxReport.runResultList0.add((double) watch.getTime(TimeUnit.MILLISECONDS));
        System.out.println(threadId + " Recent getByIndex " + watch.getTime(TimeUnit.MILLISECONDS));

        watch.reset();
        watch.start();

        try (TxnContext tx = state.store.txn(DEFAULT_STREAM_NAMESPACE)) {
            assertThat(tx.getByIndex(state.table, "secondary", String.valueOf(-threadId)).size()).isEqualTo(maxPutCountPrime);
        }


        watch.stop();
        AuxReport.runResultList1.add((double) watch.getTime(TimeUnit.MILLISECONDS));
        System.out.println(threadId + " Old getByIndex " + watch.getTime(TimeUnit.MILLISECONDS));
    }

}
