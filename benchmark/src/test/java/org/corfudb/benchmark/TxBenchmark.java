package org.corfudb.benchmark;

import org.apache.commons.lang3.RandomStringUtils;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.collections.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TxBenchmark extends UniverseHook {

    @Param({"true", "false"})
    boolean diskBacked;

    private static final int payloadSize = 1024;
    private static final int maxPutCount = 1024;
    private static final int batchSize = 1024;

    private static final int minThreadCount = 16;
    private static final int maxThreadCount = 16;

    @State(Scope.Benchmark)
    public static class BenchmarkState extends UniverseBenchmarkState {
        Table<Schema.Uuid, Schema.StringValue, ?> table;
        CorfuStore store;
        int iterationCount = 0;
    }


    public static void main(String[] args) throws RunnerException {
        String xms = "-Xms" + 4 + "G";
        String xmx = "-Xmx" + 4 + "G";

        for (int threadCount = minThreadCount; threadCount <= maxThreadCount; threadCount = threadCount * 2) {
            Options opt = jmhBuilder()
                    .threads(threadCount)
                    .jvmArgs(xms, xmx)
                    .build();

            Collection<RunResult> results = new Runner(opt).run();
        }
    }

    @Setup
    public void setup(BenchmarkState state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        setupUniverseFramework(state);

        state.store = new CorfuStore(state.corfuCluster.getLocalCorfuClient().getRuntime());
        state.table = openTable(state);
    }

    @Setup(Level.Trial)
    public void prepare(BenchmarkState state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {

    }

    private Table<Schema.Uuid, Schema.StringValue, ?> openTable(
            BenchmarkState state) throws
            InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        TableOptions.TableOptionsBuilder optionsBuilder = TableOptions.builder();

        if (diskBacked) {
            final String diskBackedDirectory = "/tmp/";
            final Path persistedCacheLocation = Paths.get(diskBackedDirectory,
                    DEFAULT_STREAM_NAME + state.iterationCount++ + diskBacked);
            CorfuOptions.PersistenceOptions persistenceOptions = CorfuOptions.PersistenceOptions.newBuilder()
                    //.setConsistencyModel(CorfuOptions.ConsistencyModel.READ_COMMITTED)
                    .setDataPath(persistedCacheLocation.toAbsolutePath().toString())
                    .build();
            optionsBuilder.persistenceOptions(persistenceOptions);
        }

        return state.store.openTable(
                DEFAULT_STREAM_NAMESPACE,
                DEFAULT_STREAM_NAME,
                Schema.Uuid.class,
                Schema.StringValue.class,
                null,
                optionsBuilder.build()
                );
    }

    @TearDown
    public void tearDown(BenchmarkState state) {
        if (state.table.corfuTable instanceof PersistedCorfuTable) {
            ((PersistedCorfuTable) state.table.corfuTable).publishStats(System.err::println);
        }
        state.corfuClient.shutdown();
        state.wf.getUniverse().shutdown();

    }


    @Benchmark
    @Measurement(iterations = 1)
    @Warmup(iterations = 1)
    public void singleThreadedWrite(BenchmarkState state) {

        for (int batchCount = 0; batchCount < maxPutCount / batchSize; batchCount++) {
            try (TxnContext tx = state.store.txn(DEFAULT_STREAM_NAMESPACE)) {
                for (int putCount = 0; putCount < batchSize; putCount++) {
                    Schema.Uuid key = Schema.Uuid.newBuilder()
                            .setLsb(batchCount * batchSize + putCount)
                            .setMsb(Thread.currentThread().getId())
                            .build();
                    final String payload = RandomStringUtils.random(payloadSize);
                    Schema.StringValue value = Schema.StringValue.newBuilder()
                            .setValue(payload)
                            .setSecondary(payload)
                            .build();
                    tx.getRecord(state.table, key);
                    tx.putRecord(state.table, key, value, null);
                }

                tx.commit();
            }
        }

    }

}
