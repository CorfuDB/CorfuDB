package org.corfudb.benchmark;

import com.google.common.reflect.TypeToken;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * A simple benchmark for {@link org.corfudb.runtime.collections.SMRMap} operations.
 *
 * Created by zlokhandwala on 5/3/19.
 */
public class SMRMapBenchmark extends AbstractCorfuBenchmark {

    /**
     * State for simple {@link org.corfudb.runtime.collections.SMRMap} benchmarks.
     * Keeps a single map, and a {@link AtomicInteger} which is used as a counter
     * to issue new keys.
     */
    public static class SMRBenchmarkState extends CorfuSingleNodeBenchmarkState {

        /**
         * A runtime to use during the iteration.
         */
        CorfuRuntime rt;

        /**
         * A map to use during each iteration.
         */
        SMRMap<Integer, Integer> map;

        /**
         * A counter to be used during each iteration.
         */
        AtomicInteger counter;

        /**
         * Initializes a new runtime, map and counter.
         */
        @Override
        public void initializeIteration() {
            super.initializeIteration();

            rt = getNewRuntime();
            map = rt.getObjectsView().build()
                    .setStreamName("test")
                    .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {
                    })
                    .open();
            counter = new AtomicInteger();
        }

        /**
         * Get the next key.
         */
        public int getNextKey() {
            return counter.getAndIncrement();
        }
    }


    /**
     * Measure the performance of pure insert operations. A single operation is performed per trial.
     */
    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    public void pureInsert(SMRBenchmarkState state) {
        state.map.blindPut(state.getNextKey(), 0);
    }

    /**
     * Measure the performance of pure insert operations multithreaded. A single operation is performed
     * per trial.
     */
    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    @Threads(5)
    public void pureInsertMultiThreaded(SMRBenchmarkState state) {
        pureInsert(state);
    }

    /**
     * Measure the performance of insert operations. A single operation is performed per trial.
     */
    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    public void insert(SMRBenchmarkState state) {
        state.map.put(state.getNextKey(), 0);
    }

    /**
     * Measure the performance of insert operations multithreaded. A single operation is performed
     * per trial.
     */
    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    @Threads(5)
    public void insertMultiThreaded(SMRBenchmarkState state) {
        insert(state);
    }

    /**
     * A more complex benchmark state which initializes 1_000 initial keys in the map,
     * which map to 1_000 random other keys in the map.
     */
    public static class SMRInitBenchmarkState extends SMRBenchmarkState {

        /**
         * The number of keys in the map.
         */
        final int num_keys = 1_000;

        /**
         * A random for generating random numbers.
         */
        Random r = new Random();

        /**
         * Initialize the map with {@link SMRInitBenchmarkState#num_keys}, each mapped
         * to another key in the map.
         */
        @Override
        public void initializeIteration() {
            super.initializeIteration();

            IntStream.range(0, num_keys).forEach(i -> map.put(i, r.nextInt(num_keys)));
        }

        /**
         * Get the next key, which is a random key in the map.
         */
        @Override
        public int getNextKey() {
            return r.nextInt(num_keys);
        }
    }

    /**
     * Measure put/get performance. First get a random key, then insert a random value to another
     * key.
     */
    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    public void getThenInsert(SMRInitBenchmarkState state) {
        Integer k = state.map.get(state.getNextKey());
        state.map.put(state.getNextKey(), k);
    }

    /**
     * Measure multithreaded put/get performance. First get a random key, then insert a random
     * value to another key.
     */
    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    @Threads(5)
    public void getThenInsertMultithreaded(SMRInitBenchmarkState state) {
        getThenInsert(state);
    }

    /**
     * Measure transactional put/get performance. Start a transaction and then perform the
     * same operations as get then insert: First get a random key, then insert a random value
     * to another key.
     */
    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    public void getThenInsertTransactional(SMRInitBenchmarkState state) {
        try {
            state.rt.getObjectsView().TXBegin();
            Integer k = state.map.get(state.getNextKey());
            state.map.put(state.getNextKey(), k);
            state.rt.getObjectsView().TXEnd();
        } catch (TransactionAbortedException ignored) {
            // ignore the abort for benchmark
        }
    }


    /**
     * Measure multithreaded transactional put/get performance. Start a transaction and then
     * perform the same operations as get then insert: First get a random key, then insert a
     * random value to another key.
     */
    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    @Threads(5)
    public void getThenInsertTransactionalMultithreaded(SMRInitBenchmarkState state) {
        getThenInsertTransactional(state);
    }
}
