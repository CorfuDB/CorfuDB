package org.corfudb.benchmarks.util;

import lombok.Getter;
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

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Data generator benchmark measures latency created by a string generation code and other utility
 * classes/methods
 */
public class DataGeneratorBenchmark {

    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(DataGeneratorBenchmark.class.getSimpleName())
                .shouldFailOnError(true)
                .build();

        new Runner(opt).run();
    }

    @Getter
    @State(Scope.Benchmark)
    public static class DataGeneratorState {
        @Param({"512", "1024", "2048"})
        private int dataSize;
    }

    @State(Scope.Benchmark)
    @Getter
    public static class JavaRandomState {
        private final Random random = new Random();
        private final Random secureRandom = new SecureRandom();
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
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 1, time = 5)
    @Threads(value = 1)
    @Fork(3)
    public void stringGenerator(Blackhole blackhole, DataGeneratorState state) {
        String value = DataGenerator.generateDataString(state.getDataSize());
        blackhole.consume(value);
    }

    /**
     * String generator performance for multiple threads
     *
     * @param blackhole jmh blackhole
     * @param state     the benchmark state
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 1, time = 5)
    @Threads(value = 4)
    @Fork(3)
    public void multiThreadedStringGenerator(Blackhole blackhole, DataGeneratorState state) {
        String value = DataGenerator.generateDataString(state.getDataSize());
        blackhole.consume(value);
    }

    /**
     * Java random generator benchmark
     *
     * @param blackhole jmh blackhole
     * @param state     the benchmark state
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 1, time = 5)
    @Threads(value = 1)
    @Fork(3)
    public void javaRandomGenerator(Blackhole blackhole, JavaRandomState state) {
        int randomValue = state.getRandom().nextInt();
        blackhole.consume(randomValue);
    }

    /**
     * Java secure random generator benchmark
     *
     * @param blackhole kmh blackhole
     * @param state     the benchmark state
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 1, time = 5)
    @Threads(value = 1)
    @Fork(3)
    public void javaSecureRandomGenerator(Blackhole blackhole, JavaRandomState state) {
        int randomValue = state.getSecureRandom().nextInt();
        blackhole.consume(randomValue);
    }
}
