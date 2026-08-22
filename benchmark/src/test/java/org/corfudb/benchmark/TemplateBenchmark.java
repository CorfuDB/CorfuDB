package org.corfudb.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class TemplateBenchmark extends UniverseHook {

    private static final int minThreadCount = 1;
    private static final int maxThreadCount = 16;

    @State(Scope.Benchmark)
    public static class BenchmarkState extends UniverseBenchmarkState {
    }

    @State(Scope.Thread)
    public static class ThreadState {
    }


    public static void main(String[] args) throws RunnerException {
        for (int threadCount = minThreadCount; threadCount <= maxThreadCount; threadCount = threadCount * 2) {
            Options opt = jmhBuilder()
                    .threads(threadCount)
                    .build();

            //Collection<RunResult> results = new Runner(opt).run();
        }
    }

    @Setup
    public void setup(BenchmarkState globalState, ThreadState threadState) {
        setupUniverseFramework(globalState);
    }

    @Setup(Level.Iteration)
    public void prepare(BenchmarkState globalState, ThreadState threadState) {
    }

    @TearDown
    public void tearDown(BenchmarkState state, ThreadState threadState) {
        state.corfuClient.shutdown();
        state.wf.getUniverse().shutdown();
    }


    @Benchmark
    public void benchmark(BenchmarkState benchmarkState, ThreadState threadState) {
    }
}
