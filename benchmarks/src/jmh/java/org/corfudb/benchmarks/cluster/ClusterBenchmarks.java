package org.corfudb.benchmarks.cluster;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.benchmarks.cluster.state.ClusterState.ClusterStateForRead;
import org.corfudb.benchmarks.cluster.state.ClusterState.ClusterStateForWrite;
import org.corfudb.runtime.collections.CorfuTable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.corfudb.benchmarks.util.DataUnit.KB;

/**
 * The benchmark measures corfu's table performance (read operation).
 * see: docs/benchmarks/corfu-table.md
 */
@Slf4j
public class ClusterBenchmarks {

    public static void main(String[] args) throws RunnerException {
        String benchmarkName = ClusterBenchmarks.class.getSimpleName();

        int warmUpIterations = 0;

        int measurementIterations = 1;
        TimeValue measurementTime = TimeValue.seconds(30);

        int threads = 1;
        int forks = 1;

        String[] dataSizes = Stream
                .of(KB.toBytes(4), KB.toBytes(128))
                .map(String::valueOf)
                .toArray(String[]::new);
        String[] dataEntries = {"1024"};
        String[] numRuntime = {"1"};
        String[] numTables = {"4"};
        String[] numServers = {"1", "3"};
        String[] txSizes = {"100"};

        Options opt = new OptionsBuilder()
                .include(benchmarkName)

                .mode(Mode.Throughput)
                .timeUnit(TimeUnit.SECONDS)

                .warmupIterations(warmUpIterations)

                .measurementIterations(measurementIterations)
                .measurementTime(measurementTime)

                .param("dataSize", dataSizes)
                .param("numEntries", dataEntries)
                .param("numServers", numServers)
                .param("numRuntime", numRuntime)
                .param("numTables", numTables)
                .param("txSize", txSizes)

                .threads(threads)
                .forks(forks)

                .shouldFailOnError(true)

                .resultFormat(ResultFormatType.CSV)
                .result("target/" + benchmarkName + ".csv")

                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void clusterBenchmarkWrite(ClusterStateForWrite state) {
        String key = String.valueOf(state.counter.getAndIncrement());
        CorfuTable<String, String> table = state.getRandomTable();
        table.put(key, state.getData());
    }

    @Benchmark
    public void clusterBenchmarkTXWrite(ClusterStateForWrite state) {
        if (state.counter.get() % state.getTxSize() == 0) {
            state.txEnd();
            state.txBegin();
        }
        String key = String.valueOf(state.counter.getAndIncrement());
        CorfuTable<String, String> table = state.getRandomTable();
        table.put(key, state.getData());
    }

    @Benchmark
    public void clusterBenchmarkRead(ClusterStateForRead state) {
        CorfuTable<String, String> table = state.getRandomTable();
        table.get(String.valueOf(state.getRandomIndex()));
    }

    @Benchmark
    public void clusterBenchmarkScanAndFilter(ClusterStateForRead state) {
        CorfuTable<String, String> table = state.getRandomTable();
        table.scanAndFilter(entry -> true);
    }

    @Benchmark
    public void clusterBenchmarkScanAndFilterByEntry(ClusterStateForRead state) {
        CorfuTable<String, String> table = state.getRandomTable();
        table.scanAndFilterByEntry(entry -> true);
    }
}
