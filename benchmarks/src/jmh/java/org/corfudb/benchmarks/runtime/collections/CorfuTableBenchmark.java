package org.corfudb.benchmarks.runtime.collections;

import static org.corfudb.benchmarks.runtime.collections.helper.CorfuTableBenchmarkHelper.DATA_SIZE;
import static org.corfudb.benchmarks.runtime.collections.helper.CorfuTableBenchmarkHelper.DATA_SIZE_FIELD;
import static org.corfudb.benchmarks.runtime.collections.helper.CorfuTableBenchmarkHelper.IN_MEM_TABLE_SIZE_FIELD;
import static org.corfudb.benchmarks.runtime.collections.helper.CorfuTableBenchmarkHelper.SMALL_TABLE_SIZE;
import static org.corfudb.benchmarks.runtime.collections.helper.CorfuTableBenchmarkHelper.TABLE_SIZE;
import static org.corfudb.benchmarks.runtime.collections.helper.CorfuTableBenchmarkHelper.TABLE_SIZE_FIELD;

import org.corfudb.benchmarks.runtime.collections.helper.CorfuTableBenchmarkHelper;
import org.corfudb.benchmarks.runtime.collections.state.EhCacheState.EhCacheStateForGet;
import org.corfudb.benchmarks.runtime.collections.state.EhCacheState.EhCacheStateForPut;
import org.corfudb.benchmarks.runtime.collections.state.HashMapState.HashMapStateForGet;
import org.corfudb.benchmarks.runtime.collections.state.HashMapState.HashMapStateForPut;
import org.corfudb.benchmarks.runtime.collections.state.RocksDbState;
import org.corfudb.benchmarks.runtime.collections.state.RocksDbState.RocksDbStateForPut;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

/**
 * CorfuTable benchmark.
 */
public class CorfuTableBenchmark {

    public static void main(String[] args) throws RunnerException {

        String benchmarkName = CorfuTableBenchmark.class.getSimpleName();

        int warmUpIterations = 1;
        TimeValue warmUpTime = TimeValue.seconds(3);

        int measurementIterations = 3;
        TimeValue measurementTime = TimeValue.seconds(10);

        int threads = 1;
        int forks = 1;

        Options opt = new OptionsBuilder()
                .include(benchmarkName)

                .mode(Mode.Throughput)
                .timeUnit(TimeUnit.SECONDS)

                .warmupIterations(warmUpIterations)
                .warmupTime(warmUpTime)

                .measurementIterations(measurementIterations)
                .measurementTime(measurementTime)

                .threads(threads)
                .forks(forks)

                .param(DATA_SIZE_FIELD, DATA_SIZE)
                .param(TABLE_SIZE_FIELD, TABLE_SIZE)
                .param(IN_MEM_TABLE_SIZE_FIELD, SMALL_TABLE_SIZE)

                .shouldFailOnError(true)

                .jvmArgsAppend("-Xms4g", "-Xmx4g")

                .resultFormat(ResultFormatType.CSV)
                .result("target/" + benchmarkName + ".csv")

                .build();

        new Runner(opt).run();
    }

    /**
     * Put operation benchmark for RocksDb
     *
     * @param state     benchmark state
     * @param blackhole jmh blackhole
     */
    @Benchmark
    public void rocksDbPut(RocksDbStateForPut state, Blackhole blackhole) {
        CorfuTableBenchmarkHelper helper = state.getHelper();
        Integer key = helper.generate();
        String result = helper.getUnderlyingMap().put(key, helper.generateValue());
        blackhole.consume(result);
    }

    /**
     * Put operation benchmark for EhCache
     *
     * @param state     benchmark state
     * @param blackhole jmh blackhole
     */
    @Benchmark
    public void ehCachePut(EhCacheStateForPut state, Blackhole blackhole) {
        CorfuTableBenchmarkHelper helper = state.getHelper();
        String result = helper.getTable().put(helper.generate(), helper.generateValue());
        blackhole.consume(result);
    }

    /**
     * Get operation benchmark for RocksDb
     *
     * @param state     benchmark state
     * @param blackhole jmh blackhole
     */
    @Benchmark
    public void rocksDbGet(RocksDbState.RocksDbStateForGet state, Blackhole blackhole) {
        CorfuTableBenchmarkHelper helper = state.getHelper();
        int key = helper.generate();
        String value = helper.getTable().get(key);

        if (value == null) {
            throw new IllegalStateException("The value not found in the cache. Key: " + key);
        }

        blackhole.consume(value);
    }

    /**
     * Get operation benchmark for ehCache
     *
     * @param state     benchmark state
     * @param blackhole jmh blackhole
     */
    @Benchmark
    public void ehCacheGet(EhCacheStateForGet state, Blackhole blackhole) {
        CorfuTableBenchmarkHelper helper = state.getHelper();
        int key = helper.generate();
        String value = helper.getTable().get(key);
        if (value == null) {
            throw new IllegalStateException("The value not found in the cache. Key: " + key);
        }
        blackhole.consume(value);
    }

    /**
     * Put operation benchmark for HashMap
     *
     * @param state     benchmark state
     * @param blackhole jmh blackhole
     */
    @Benchmark
    public void hashMapPut(HashMapStateForPut state, Blackhole blackhole) {
        CorfuTableBenchmarkHelper helper = state.getHelper();
        String value = helper.getTable().put(helper.generate(), helper.generateValue());
        blackhole.consume(value);
    }

    /**
     * Get operation benchmark for HashMap
     *
     * @param state     benchmark state
     * @param blackhole jmh blackhole
     */
    @Benchmark
    public void hashMapGet(HashMapStateForGet state, Blackhole blackhole) {
        CorfuTableBenchmarkHelper helper = state.getHelper();
        String value = helper.getTable().get(helper.generate());
        blackhole.consume(value);
    }

    /**
     * Get/Put 50x50 load for HashMap
     *
     * @param state     benchmark state
     * @param blackhole jmh blackhole
     */
    @Benchmark
    public void readWriteRatio50x50(HashMapStateForGet state, Blackhole blackhole) {
        CorfuTableBenchmarkHelper helper = state.getHelper();
        int rndValue = helper.generate();
        String rndValueStr = String.valueOf(rndValue);
        String value = helper.getTable().put(rndValue, rndValueStr);
        blackhole.consume(value);

        value = helper.getTable().get(rndValue);
        blackhole.consume(value);
    }
}
