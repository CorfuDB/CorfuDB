package org.corfudb.benchmark;

import org.apache.commons.lang3.RandomStringUtils;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.universe.UniverseManager;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseParams;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class LongevityBenchmark extends GenericIntegrationTest {

    private int payloadSize = 10;
    private int syncCount = 256;
    private int maxPutCount = 128;

    private static final int threadCount = 16;
    private static final int minHeap = 512 + 256;
    private static final int maxHeap = minHeap;

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        AtomicBoolean shutdown = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(threadCount);
        Table<Schema.Uuid, Schema.StringValue, ?> table;
        CorfuStore store;
        CorfuClient corfuClient;
        CorfuCluster corfuCluster;
        UniverseManager.UniverseWorkflow<?> workflow;
    }


    public static void main(String[] args) throws RunnerException {
        for (int heapSize = minHeap; heapSize <= maxHeap; heapSize = heapSize * 2) {
            String xms = "-Xms" + heapSize + "M";
            String xmx = "-Xmx" + heapSize + "M";

            String sessionName = String.format("{date}-%s-Threads-%d-%s",
                    MethodHandles.lookup().lookupClass(), threadCount, xmx);
            Options opt = new OptionsBuilder()
                    .include(LongevityBenchmark.class.getSimpleName())
                    //.verbosity(SILENT)
                    .addProfiler("gc")
                    .forks(1)
                    .timeout(TimeValue.hours(3))
                    .jvmArgs(xms, xmx
                            ,"-agentpath:/home/vjeko/Downloads/YourKit-JavaProfiler-2022.3/bin/linux-x86-64/libyjpagent.so=onexit=snapshot,snapshot_name_format=" + sessionName
                    )
                    .threads(threadCount)
                    .build();

            System.out.println("Heap Size: " + heapSize);
            Collection<RunResult> results = new Runner(opt).run();
            for (RunResult result : results) {
                System.out.println(result.getPrimaryResult().toString());
                final Result primary = result.getPrimaryResult();
                System.out.println(primary.getLabel() + ": " + primary.getScore() + " +/-" +
                        primary.getScoreError() + " " + primary.getScoreUnit());
            }
        }
    }

    @Setup
    public void setup(BenchmarkState state) throws
            InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        universeManager = UniverseManager.builder()
                .testName(TxLongBenchmark.class.getSimpleName())
                .universeMode(Universe.UniverseMode.DOCKER)
                .corfuServerVersion(APP_UTIL.getAppVersion())
                .build();
        state.workflow = universeManager.workflow();
        state.workflow.deploy();

        UniverseParams params = state.workflow.getFixture().data();

        state.corfuCluster = state.workflow.getUniverse()
                .getGroup(params.getGroupParamByIndex(0).getName());
        state.corfuClient = state.corfuCluster.getLocalCorfuClient();
        state.store = new CorfuStore(state.corfuCluster.getLocalCorfuClient().getRuntime());
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
        state.corfuClient.shutdown();
        state.workflow.getUniverse().shutdown();
    }

    private synchronized CountDownLatch decrement(BenchmarkState state, Timestamp timestamp) {
        CountDownLatch current = state.latch;
        long count = current.getCount();
        if (count == 1) {
            state.latch = new CountDownLatch(threadCount);
            System.out.println("Resetting the latch.");
            System.out.println("Trimming address space at " + (timestamp.getSequence() - syncCount));
            trimAddressSpace(state.corfuClient.getRuntime(),
                    Token.of(timestamp.getEpoch(), timestamp.getSequence() - syncCount));
        }
        current.countDown();
        return current;
    }

    private void trimAddressSpace(CorfuRuntime runtime, Token token) {
        runtime.getAddressSpaceView().prefixTrim(token);
        runtime.getAddressSpaceView().gc();
        runtime.getAddressSpaceView().invalidateServerCaches();
        runtime.getAddressSpaceView().invalidateClientCache();
    }


    @Benchmark
    @Measurement(iterations = 1)
    @Warmup(iterations = 1)
    public void singleThreadedWrite(BenchmarkState state) throws InterruptedException {
        long txCount = 0L;
        while (!state.shutdown.get()) {
            try (TxnContext tx = state.store.txn(Fixtures.TestFixtureConst.DEFAULT_STREAM_NAMESPACE)) {
                for (int putCount = 0; putCount < maxPutCount; putCount++) {
                    Schema.Uuid key = Schema.Uuid.newBuilder().setLsb(Thread.currentThread().getId())
                            .setMsb(putCount).build();
                    final String payload = RandomStringUtils.random(payloadSize);
                    Schema.StringValue value = Schema.StringValue.newBuilder()
                            .setValue(payload)
                            .setSecondary(payload)
                            .build();
                    tx.getRecord(state.table, key);
                    tx.putRecord(state.table, key, value, null);
                }

                Timestamp timestamp = tx.commit();

                txCount++;
                if (txCount % 256 == 0) {
                    decrement(state, timestamp).await();
                }
            }


        }
    }

}
