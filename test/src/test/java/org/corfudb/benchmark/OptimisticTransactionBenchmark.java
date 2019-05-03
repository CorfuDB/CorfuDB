package org.corfudb.benchmark;

import com.google.common.reflect.TypeToken;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark which measures the performance of default (optimistic) transactions.
 *
 * Created by zlokhandwala on 5/3/19.
 */
public class OptimisticTransactionBenchmark extends AbstractCorfuBenchmark {

    public static class OptimisticBenchmarkState extends CorfuSingleNodeBenchmarkState {

        /**
         * A runtime to use during the iteration.
         */
        CorfuRuntime rt;

        @Param({"10", "100"})
        int NUM_MAPS;

        @Param({"10", "100"})
        int NUM_KEYS;

        @Param({"1", "100"})
        int NUM_OPERATIONS;

        /**
         * Maps to use.
         */
        List<Map<Integer, Integer>> maps;

        /**
         * A random for generating random numbers.
         */
        Random r = new Random();

        /**
         * Initializes a new runtime, map and counter.
         */
        @Override
        public void initializeIteration() {
            super.initializeIteration();

            rt = getNewRuntime();

            // Every map will be initialized with this map
            Map<Integer, Integer> initialMap = new HashMap<>();
            for (int i = 0; i < NUM_KEYS; i++) {
                initialMap.put(i, i);
            }

            maps = new ArrayList<>();
            for (int i = 0; i < NUM_MAPS; i++) {
                maps.add(rt.getObjectsView().build()
                        .setStreamName(Integer.toString(i))
                        .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {
                        })
                        .open());
                // Initialize the map with initial map
                maps.get(i).putAll(initialMap);
            }

        }

        /**
         * Get the next key.
         */
        int getNextKey() {
            return r.nextInt(NUM_KEYS);
        }

        /**
         * Get the next map.
         */
        @Nonnull
        Map<Integer, Integer> getNextMap() {
            return maps.get(r.nextInt(NUM_MAPS));
        }

        /**
         * A simple interface which defines the transaction to be executed.
         */
        @FunctionalInterface
        interface BenchmarkedTransaction {
            void doTransaction();
        }

        /**
         * Do a transaction.
         */
        void doTransaction(@Nonnull TransactionCounters tc, @Nonnull BenchmarkedTransaction tx) {
            while (true) {
                tc.transactions++;
                try {
                    rt.getObjectsView().TXBegin();
                    tx.doTransaction();
                    rt.getObjectsView().TXEnd();
                    return;
                } catch (TransactionAbortedException tae) {
                    tc.aborts++;
                }
            }
        }
    }

    /**
     * Counters for measuring the number of actual transactions dispatched and how many were aborted.
     * The {@link OptimisticBenchmarkState#doTransaction(TransactionCounters, OptimisticBenchmarkState.BenchmarkedTransaction)}
     * method measures goodput, so these counters give us actual transaction and abort rate.
     */
    @AuxCounters
    @State(Scope.Thread)
    public static class TransactionCounters {
        public long transactions;
        public long aborts;

        @Setup(Level.Iteration)
        public void cleanup() {
            transactions = 0;
            aborts = 0;
        }
    }

    /**
     * Measure transaction performance. Each transaction reads a key and write a key (in a different map).
     * The number of maps, keys and operations per transaction vary depending on the given parameters,
     * but the read:write ratio is fixed at 1:1.
     */
    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    @Threads(5)
    public void readWriteDiffMap(OptimisticBenchmarkState state, TransactionCounters tc, Blackhole bh) {
        state.doTransaction(tc, () -> {
            for (int i = 0; i < state.NUM_OPERATIONS; i++) {
                Integer k = state.getNextMap().get(state.getNextKey());
                bh.consume(state.getNextMap().put(state.getNextKey(), k));
            }
        });
    }

}
