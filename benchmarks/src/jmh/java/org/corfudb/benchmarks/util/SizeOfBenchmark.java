package org.corfudb.benchmarks.util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.ehcache.sizeof.SizeOf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Data generator benchmark measures latency created by a string generation code and other utility
 * classes/methods
 */
public class SizeOfBenchmark {

    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(SizeOfBenchmark.class.getSimpleName())
                .shouldFailOnError(true)
                .build();

        new Runner(opt).run();
    }

    @Getter
    @State(Scope.Benchmark)
    public static class DataGeneratorState {
        @Param({"1000", "10000"})
        private int dataSize;
    }

    /**
     * String generator performance for a single thread
     *
     * @param blackhole jmh blackhole
     * @param state     the benchmark state
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 3)
    @Measurement(iterations = 1, time = 10)
    @Threads(value = 1)
    @Fork(1)
    public void stringGenerator(Blackhole blackhole, DataGeneratorState state) {
        String value = DataGenerator.generateDataString(state.getDataSize());
        SizeOf sizeOf = SizeOf.newInstance();
        long size = sizeOf.deepSizeOf(new MyData(value));
        blackhole.consume(size);
    }

    @AllArgsConstructor
    private static class MyData {
        private final String value;
    }
}
