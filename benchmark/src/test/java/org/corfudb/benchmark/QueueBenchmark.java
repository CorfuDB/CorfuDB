package org.corfudb.benchmark;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.corfudb.runtime.collections.CorfuQueue;
import org.corfudb.runtime.collections.CorfuQueue.CorfuQueueRecord;
import org.corfudb.runtime.collections.CorfuQueue.CorfuRecordId;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.universe.UniverseManager;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseParams;
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
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class QueueBenchmark extends GenericIntegrationTest {

    private int payloadSize = 10;
    private int maxBatchCount = 4;

    private static final int minThreadCount = 4;
    private static final int maxThreadCount = 64;
    private static int maxEntries = 10_000;


    @State(Scope.Benchmark)
    public static class BenchmarkState {
        AtomicInteger threadCount = new AtomicInteger(0);
        AtomicInteger entriesToProduce = new AtomicInteger(maxEntries);
        CorfuQueue queue;
        CorfuClient corfuClient;
        CorfuCluster corfuCluster;
        UniverseManager.UniverseWorkflow<?> workflow;
        CorfuRecordId id;
    }


    public static void main(String[] args) throws RunnerException {
        for (int threadCount = minThreadCount; threadCount <= maxThreadCount; threadCount = threadCount * 2) {
            Options opt = new OptionsBuilder()
                    .include(QueueBenchmark.class.getSimpleName())
                    //.verbosity(SILENT)
                    .addProfiler("gc")
                    .forks(1)
                    .jvmArgs("-Xms1G", "-Xmx1G"
                            //,"-agentpath:/home/vjeko/Downloads/YourKit-JavaProfiler-2022.3/bin/linux-x86-64/libyjpagent.so=onexit=snapshot"
                            )
                    .threads(threadCount)
                    .build();

            System.out.println("Thread Count: " + threadCount);
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
        state.queue = new CorfuQueue(state.corfuClient.getRuntime(), this.getClass().getSimpleName());
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


    @Setup(Level.Iteration)
    public void setupIteration(BenchmarkState state) {
        state.threadCount = new AtomicInteger(0);
        state.entriesToProduce = new AtomicInteger(maxEntries);
        state.id = state.queue.entryList().stream().map(CorfuQueueRecord::getRecordId).max(CorfuRecordId::compareTo)
                .orElse(new CorfuRecordId(0, 0));
    }

    @TearDown
    public void tearDown(BenchmarkState state) {
        state.corfuClient.shutdown();
        state.workflow.getUniverse().shutdown();
    }

    private void producer(BenchmarkState state) {
        while (true) {
            int size = state.entriesToProduce.addAndGet(-maxBatchCount);
            if (size < 0) {
                System.out.println(Thread.currentThread().getId() + " Done producing.");
                break;
            }
            if (size % 500 == 0) {
                System.out.println(Thread.currentThread().getId() + " Remaining: " + size);
            }

            state.corfuClient.getObjectsView().TXBegin();

            for (int batchCount = 0; batchCount < maxBatchCount; batchCount++) {
                String payload = StringUtils.repeat('0', payloadSize);
                Schema.StringValue value = Schema.StringValue.newBuilder()
                        .setValue(payload)
                        .setSecondary(payload)
                        .build();
                state.queue.enqueue(value.toByteString());
            }

            state.corfuClient.getObjectsView().TXEnd();
        }
    }

    private void consumer(BenchmarkState state) {
        int entriesConsumed = 0;
        CorfuRecordId id = state.id;
        System.out.println(Thread.currentThread().getId() + " CorfuRecordId: " + id);
        while (entriesConsumed < maxEntries) {
            try {
                state.corfuClient.getObjectsView().TXBegin();
                List<CorfuQueueRecord> entries = state.queue.entryList(id, maxBatchCount);
                if (entries.isEmpty()) {
                    System.out.println(Thread.currentThread().getId() +
                            " Entries is empty. Size: " + entriesConsumed);
                    continue;
                }
                assert(entries.size() == maxBatchCount);
                entriesConsumed += entries.size();
                id = entries.get(entries.size() - 1).getRecordId();
                if (entriesConsumed % 500 == 0) {
                    System.out.println(Thread.currentThread().getId() +
                            " Consumed: " + entriesConsumed);
                }
            } catch (TrimmedException trimmedEx) {
                System.out.println(Thread.currentThread().getId() +
                        " Trimmed Exception...");
            } finally {
                state.corfuClient.getObjectsView().TXEnd();
            }
        }
    }

    @Benchmark
    @Measurement(iterations = 1)
    @Warmup(iterations = 1)
    public void consumerProducer(BenchmarkState state) {
        long threadCount = state.threadCount.incrementAndGet();
        if (threadCount % 2 == 0) {
            consumer(state);
        } else {
            producer(state);
        }
    }

}
