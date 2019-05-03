package org.corfudb.benchmark;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Junit bridge for jmh.
 *
 * To implement your own benchmark class, subclass (extend) this class and provide your own state by
 * extending {@link CorfuSingleNodeBenchmarkState} as a static inner class.
 *
 * Annotate your benchmark functions with {@link org.openjdk.jmh.annotations.Benchmark}.
 * This will mark them for being run when the {@link this#runBenchmarks()} junit test is run.
 *
 * Created by zlokhandwala on 5/3/19.
 */
public abstract class AbstractCorfuBenchmark {

    private static final int WARM_UP_TIME_SECONDS = 5;
    private static final int MEASUREMENT_TIME_SECONDS = 5;

    private static final Mode MODE = Mode.Throughput;

    /**
     * Benchmark runner for the class.
     */
    @Test
    public void runBenchmarks() throws Exception {

        Options opt = new OptionsBuilder()
                .include(this.getClass().getName() + ".*")
                .mode(MODE)
                .timeUnit(TimeUnit.MILLISECONDS)

                // Overrides annotation options
                .warmupTime(TimeValue.seconds(WARM_UP_TIME_SECONDS))
                .measurementTime(TimeValue.seconds(MEASUREMENT_TIME_SECONDS))
                .forks(1)
                .shouldFailOnError(true)
                .shouldDoGC(true)
                .build();

        new Runner(opt).run();
    }

}
