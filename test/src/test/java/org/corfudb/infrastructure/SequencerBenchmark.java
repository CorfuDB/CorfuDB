package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.test.benchmark.AbstractCorfuBenchmark;
import org.corfudb.test.benchmark.CorfuBenchmarkState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/** A set of benchmarks for the sequencer server. */
public class SequencerBenchmark extends AbstractCorfuBenchmark {

    public static class SequencerBenchmarkState extends CorfuBenchmarkState {
        /** A runtime to use during the iteration. */
        CorfuRuntime rt;

        @Override
        public void initializeIteration() {
            super.initializeIteration();

            rt = getNewRuntime();
        }

        UUID nextStream() {
            return UUID.randomUUID();
        }
    }

    /** Measure token acquisition (non-transactional) performance.
     */
    @Benchmark
    @Warmup(iterations=5)
    @Measurement(iterations=10)
    @Threads(5)
    public void getToken(SequencerBenchmarkState state, Blackhole bh) {
        bh.consume(state.rt.getSequencerView().nextToken(Collections.singleton(state.nextStream()), 1));
    }

    /** Measure token query performance.
     */
    @Benchmark
    @Warmup(iterations=5)
    @Measurement(iterations=10)
    @Threads(5)
    public void queryToken(SequencerBenchmarkState state, Blackhole bh) {
        bh.consume(state.rt.getSequencerView().nextToken(Collections.singleton(state.nextStream()), 0));
    }

    public static class TransactionalSequencerBenchmarkState extends CorfuBenchmarkState {
        /** A runtime to use during the iteration. */
        CorfuRuntime rt;

        @Param({"1", "10"})
        int STREAMS;

        @Param({"1", "10", "100"})
        int CONFLICTS_PER_STREAM;

        AtomicLong counter;

        @Override
        public void initializeIteration() {
            super.initializeIteration();

            rt = getNewRuntime();
            counter = new AtomicLong();
        }

        TxResolutionInfo getResolutionInfo() {
            Set<byte[]> conflictSetParameters = new HashSet<>();
            for (int i = 0; i < CONFLICTS_PER_STREAM; i++) {
                byte[] bytes = new byte[8];
                bytes[0] = (byte) i;
                conflictSetParameters.add(bytes);
            }

            Map<UUID, Set<byte[]>> conflictParameters = new HashMap<>();
            for (int i = 0; i < STREAMS; i++) {
                UUID uuid = UUID.randomUUID();
                conflictParameters.put(uuid, conflictSetParameters);
            }


            return new TxResolutionInfo(UUID.randomUUID(),
                    counter.getAndIncrement(),
                    conflictParameters,
                    conflictParameters);
        }

    }

    /** Measure transactional token query performance.
     */
    @Benchmark
    @Warmup(iterations=5)
    @Measurement(iterations=10)
    @Threads(5)
    public void txToken(TransactionalSequencerBenchmarkState state, Blackhole bh) {
        TxResolutionInfo resolutionInfo = state.getResolutionInfo();
        bh.consume(state.rt.getSequencerView().nextToken(resolutionInfo.getConflictSet().keySet(),
                1, resolutionInfo));
    }
}
