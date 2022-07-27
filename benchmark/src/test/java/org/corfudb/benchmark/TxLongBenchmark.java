package org.corfudb.benchmark;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.universe.UniverseManager;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.benchmark.Schema;
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
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;

import static org.openjdk.jmh.runner.options.VerboseMode.SILENT;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms1G", "-Xmx1G"})
public class TxLongBenchmark extends GenericIntegrationTest {

    CorfuClient corfuClient;
    UniverseManager.UniverseWorkflow<Fixture<UniverseParams>> wf;

    private final int longTxCount = 16;
    private final int longTxPutCount = 1024;
    private final int shortTxCount = 128;
    private final int shortTxPutCount = 64;

    private final int payloadSize = 10;

    private static final int heapSize = 2048;
    private static final String xms = "-Xms" + heapSize + "M";
    private static final String xmx = "-Xmx" + heapSize + "M";
    private static final int minThreadCount = 1;
    private static final int maxThreadCount = 32;
    private static final int maxLongRunningThreads = 1;

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        AtomicInteger longRunningThread = new AtomicInteger(maxLongRunningThreads);
        LongAccumulator longTxMaxDuration = new LongAccumulator(Long::max, 0);

        Table<Schema.Uuid, Schema.StringValue, ?> table;
        CorfuStore store;
    }

    public volatile static int threadCount = 4;


    public static void main(String[] args) throws RunnerException {
        for (int threads = minThreadCount; threads <= maxThreadCount; threads = threads * 2) {
            threadCount = threads;
            String sessionName = String.format("{date}-%s-Threads-%d-%s",
                    MethodHandles.lookup().lookupClass(), threadCount, xmx);
            System.out.println("Thread Count: " + threadCount);

            Options opt = new OptionsBuilder()
                    .include(TxLongBenchmark.class.getSimpleName())
                    //.verbosity(SILENT)
                    .addProfiler("gc")
                    .jvmArgs(xms, xmx
                            ,"-agentpath:/home/vjeko/Downloads/YourKit-JavaProfiler-2022.3/bin/linux-x86-64/libyjpagent.so=onexit=snapshot,snapshot_name_format=" + sessionName
                    )
                    .forks(1)
                    .threads(threadCount)
                    .build();

            Collection<RunResult> results = new Runner(opt).run();
        }

    }

    @Setup(Level.Iteration)
    public void prepare(BenchmarkState state) {
        state.longTxMaxDuration.reset();
        state.longRunningThread.set(maxLongRunningThreads);
    }

    @Setup
    public void setup(BenchmarkState state) throws
            InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        universeManager = UniverseManager.builder()
                .testName(TxLongBenchmark.class.getSimpleName())
                .universeMode(Universe.UniverseMode.DOCKER)
                .corfuServerVersion(APP_UTIL.getAppVersion())
                .build();
        wf = universeManager.workflow();
        wf.deploy();

        UniverseParams params = wf.getFixture().data();

        CorfuCluster corfuCluster = wf.getUniverse()
                .getGroup(params.getGroupParamByIndex(0).getName());
        corfuClient = corfuCluster.getLocalCorfuClient();
        state.store = new CorfuStore(corfuCluster.getLocalCorfuClient().getRuntime());
        state.table = openTable(state.store);
    }

    private Table<Schema.Uuid, Schema.StringValue, ?> openTable(CorfuStore store) throws
            InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return store.openTable(
                Fixtures.TestFixtureConst.DEFAULT_STREAM_NAMESPACE,
                Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME,
                Schema.Uuid.class,
                Schema.StringValue.class,
                null,
                TableOptions.builder().build());
    }

    @TearDown
    public void tearDown(BenchmarkState state) {
        System.out.println();
        String formatted = String.format("Threads %d | Max TX Duration = %d ms", threadCount, state.longTxMaxDuration.get());
        System.out.println(formatted);

        corfuClient.shutdown();
        wf.shutdown();
    }


    public void shortRunning(BenchmarkState state) {
        for (int txCount = 0; txCount < shortTxCount; txCount++) {
            try {
                TxnContext tx = state.store.txn(Fixtures.TestFixtureConst.DEFAULT_STREAM_NAMESPACE);

                for (int putCount = 0; putCount < shortTxPutCount; putCount++) {
                    Schema.Uuid key = Schema.Uuid.newBuilder().setLsb(Thread.currentThread().getId())
                            .setMsb(putCount + 1_000_000 * (txCount)).build();
                    final String payload = RandomStringUtils.random(payloadSize);
                    Schema.StringValue value = Schema.StringValue.newBuilder()
                            .setValue(payload)
                            .setSecondary(payload)
                            .build();
                    tx.getRecord(state.table, key);
                    tx.putRecord(state.table, key, value, null);
                }

                tx.commit();
            } catch(TransactionAbortedException ex){
                if (ex.getAbortCause() != AbortCause.CONFLICT) {
                    throw ex;
                }
            }
        }
    }
    public void longRunning(BenchmarkState state) {
        for (int txCount = 0; txCount < longTxCount; txCount++) {
            try {
                TxnContext tx = state.store.txn(Fixtures.TestFixtureConst.DEFAULT_STREAM_NAMESPACE);

                for (int putCount = 0; putCount < longTxPutCount; putCount++) {
                    Schema.Uuid key = Schema.Uuid.newBuilder().setLsb(Thread.currentThread().getId())
                            .setMsb(putCount + 1_000_000 * (txCount)).build();
                    final String payload = RandomStringUtils.random(payloadSize);
                    Schema.StringValue value = Schema.StringValue.newBuilder()
                            .setValue(payload)
                            .setSecondary(payload)
                            .build();

                    tx.getRecord(state.table, key);
                    tx.putRecord(state.table, key, value, null);
                }

                tx.commit();
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
    public void longTx(BenchmarkState state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        final StopWatch watch = new StopWatch();
        final int count = state.longRunningThread.decrementAndGet();

        watch.start();
        if (count == 0) {
            longRunning(state);
        } else {
            shortRunning(state);
        }
        watch.stop();

        long txDuration = watch.getTime(TimeUnit.MILLISECONDS);
        state.longTxMaxDuration.accumulate(txDuration);
    }

}
