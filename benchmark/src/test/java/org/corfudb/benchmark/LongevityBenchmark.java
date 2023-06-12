package org.corfudb.benchmark;

import org.apache.commons.lang3.RandomStringUtils;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class LongevityBenchmark extends UniverseHook {

    private final int payloadSize = 10;
    private final int syncCount = 256;
    private final int maxPutCount = 128;

    private static final int threadCount = 16;
    private static final int minHeap = 512 + 256;
    private static final int maxHeap = minHeap;

    @State(Scope.Benchmark)
    public static class BenchmarkState extends UniverseBenchmarkState {
        AtomicBoolean shutdown = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(threadCount);
        Table<Schema.Uuid, Schema.StringValue, ?> table;
        CorfuStore store;
    }

    public static void main(String[] args) throws RunnerException {
        for (int heapSize = minHeap; heapSize <= maxHeap; heapSize = heapSize * 2) {
            String xms = "-Xms" + heapSize + "M";
            String xmx = "-Xmx" + heapSize + "M";

            String sessionName = String.format("{date}-%s-Threads-%d-%s",
                    MethodHandles.lookup().lookupClass(), threadCount, xmx);

            Options opt = jmhBuilder()
                    .threads(threadCount)
                    .jvmArgs(xms, xmx
                            ,"-agentpath:/home/vjeko/Downloads/YourKit-JavaProfiler-2022.3/bin/linux-x86-64/libyjpagent.so=onexit=snapshot,snapshot_name_format=" + sessionName
                    )
                    .build();

            //Collection<RunResult> results = new Runner(opt).run();
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
    }


    private Table<Schema.Uuid, Schema.StringValue, ?> openTable(CorfuStore store) throws
            InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return store.openTable(
                DEFAULT_STREAM_NAMESPACE,
                DEFAULT_STREAM_NAME,
                Schema.Uuid.class,
                Schema.StringValue.class,
                null,
                TableOptions.builder().build());
    }

    @TearDown
    public void tearDown(BenchmarkState state) {
        state.corfuClient.shutdown();
        state.wf.getUniverse().shutdown();
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
            try (TxnContext tx = state.store.txn(DEFAULT_STREAM_NAMESPACE)) {
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
