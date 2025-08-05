package org.corfudb.benchmark;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
public class SecondaryIndexBenchmark extends UniverseHook {

    private final int shortTxCount = 128;
    private final int shortTxPutCount = 64;
    private final int payloadSize = 10;

    private static final int minThreadCount = 1;
    private static final int maxThreadCount = 32;
    private static final int maxLongRunningThreads = 1;

    @State(Scope.Benchmark)
    public static class BenchmarkState extends UniverseBenchmarkState {
        AtomicInteger longRunningThread = new AtomicInteger(maxLongRunningThreads);
        LongAccumulator longTxMaxDuration = new LongAccumulator(Long::max, 0);
        Table<Schema.Uuid, Schema.IndexedStringValue, ?> table;
        CorfuStore store;
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

    @Setup
    public void setup(BenchmarkState state) {
        setupUniverseFramework(state);
    }


    @Setup(Level.Iteration)
    public void prepare(BenchmarkState state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        state.store = new CorfuStore(state.corfuCluster.getLocalCorfuClient().getRuntime());
        state.table = openTable(state.store);

        state.longTxMaxDuration.reset();
        state.longRunningThread.set(maxLongRunningThreads);
    }

    private Table<Schema.Uuid, Schema.IndexedStringValue, ?> openTable(CorfuStore store) throws
            InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return store.openTable(
                DEFAULT_STREAM_NAMESPACE,
                DEFAULT_STREAM_NAME,
                Schema.Uuid.class,
                Schema.IndexedStringValue.class,
                null,
                TableOptions.fromProtoSchema(Schema.IndexedStringValue.class));
    }

    @TearDown
    public void tearDown(BenchmarkState state, BenchmarkParams params) {
        System.out.println();
        String formatted = String.format("Threads %d | Max TX Duration = %d ms",
                params.getThreads(), state.longTxMaxDuration.get());
        System.out.println(formatted);
        AuxReport.runResult = state.longTxMaxDuration.get();

        state.corfuClient.shutdown();
        state.wf.shutdown();
    }


    public void shortRunning(BenchmarkState state) {
        for (int txCount = 0; txCount < shortTxCount; txCount++) {
            TxnContext tx = state.store.txn(DEFAULT_STREAM_NAMESPACE);

            for (int putCount = 0; putCount < shortTxPutCount; putCount++) {
                final String payload = RandomStringUtils.random(payloadSize);
                Schema.Uuid key = Schema.Uuid.newBuilder()
                        .setLsb(Thread.currentThread().getId())
                        .setMsb(putCount).build();
                final String secondary = StringUtils.repeat("0", payloadSize);
                Schema.IndexedStringValue value = Schema.IndexedStringValue.newBuilder()
                        .setValue(payload)
                        .setSecondary(secondary)
                        .build();
                tx.getRecord(state.table, key);
                tx.putRecord(state.table, key, value, null);
            }

            tx.commit();
        }
    }

    public void secondaryIndex(BenchmarkState state) {
        for (int txCount = 0; txCount < 10; txCount++) {
            try {
                TxnContext tx = state.store.txn(DEFAULT_STREAM_NAMESPACE);
                final String secondary = StringUtils.repeat("0", payloadSize);
                List<?> result =  tx.getByIndex(state.table, "secondary", secondary);
                tx.txAbort();
            } catch(TransactionAbortedException ex){
                if (ex.getAbortCause() != AbortCause.CONFLICT) {
                    throw ex;
                }
            }
        }
    }

    @Benchmark
    @Measurement(iterations = 1)
    @Warmup(iterations = 1)
    public void getByIndex(BenchmarkState state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        final StopWatch watch = new StopWatch();
        final int count = state.longRunningThread.decrementAndGet();

        watch.start();
        if (count == 0) {
            //entryStream(state);
            secondaryIndex(state);
        } else {
            shortRunning(state);
        }

        watch.stop();

        long txDuration = watch.getTime(TimeUnit.MILLISECONDS);
        state.longTxMaxDuration.accumulate(txDuration);
    }

}
