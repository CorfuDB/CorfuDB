package org.corfudb.benchmark;

import org.apache.commons.lang3.StringUtils;
import org.corfudb.runtime.collections.CorfuQueue;
import org.corfudb.runtime.collections.CorfuQueue.CorfuQueueRecord;
import org.corfudb.runtime.collections.CorfuQueue.CorfuRecordId;
import org.corfudb.runtime.exceptions.TrimmedException;
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

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class QueueBenchmark extends UniverseHook {

    private final int payloadSize = 10;
    private final int maxBatchCount = 4;

    private static final int minThreadCount = 4;
    private static final int maxThreadCount = 64;
    private static final int maxEntries = 10_000;

    public static void main(String[] args) throws RunnerException {
        for (int threadCount = minThreadCount; threadCount <= maxThreadCount; threadCount = threadCount * 2) {
            Options opt = jmhBuilder()
                    .threads(threadCount)
                    .build();

            //Collection<RunResult> results = new Runner(opt).run();
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState extends UniverseBenchmarkState {
        AtomicInteger threadCount = new AtomicInteger(0);
        AtomicInteger entriesToProduce = new AtomicInteger(maxEntries);
        CorfuQueue queue;
        CorfuRecordId id;
    }

    @Setup
    public void setup(BenchmarkState state) {
        setupUniverseFramework(state);
    }

    @Setup(Level.Iteration)
    public void prepare(BenchmarkState state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        state.queue = new CorfuQueue(state.corfuClient.getRuntime(), this.getClass().getSimpleName());
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
        state.wf.getUniverse().shutdown();
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
