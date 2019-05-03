package org.corfudb.benchmark;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.benchmark.AbstractCorfuBenchmark;
import org.corfudb.benchmark.CorfuSingleNodeBenchmarkState;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A set of benchmarks for the sequencer server.
 *
 * Created by zlokhandwala on 5/3/19.
 */
public class SequencerBenchmark extends AbstractCorfuBenchmark {

    public static class SequencerBenchmarkState extends CorfuSingleNodeBenchmarkState {
        /**
         * A runtime to use during the iteration.
         */
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
//
//    /**
//     * Measure token acquisition (non-transactional) performance.
//     */
//    @Benchmark
//    @Warmup(iterations = 5)
//    @Measurement(iterations = 10)
//    @Threads(5)
//    public void getToken(SequencerBenchmarkState state, Blackhole bh) {
//        bh.consume(state.rt.getSequencerView().next(state.nextStream()));
//    }
//
//    /**
//     * Measure token query performance.
//     */
//    @Benchmark
//    @Warmup(iterations = 5)
//    @Measurement(iterations = 10)
//    @Threads(5)
//    public void queryToken(SequencerBenchmarkState state, Blackhole bh) {
//        bh.consume(state.rt.getSequencerView().next(state.nextStream()));
//    }

    public static class TransactionalSequencerBenchmarkState extends CorfuSingleNodeBenchmarkState {
        /**
         * A runtime to use during the iteration.
         */
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
            final int byte_size = 8;
            for (int i = 0; i < CONFLICTS_PER_STREAM; i++) {
                byte[] bytes = new byte[byte_size];
                bytes[0] = (byte) i;
                conflictSetParameters.add(bytes);
            }

            Map<UUID, Set<byte[]>> conflictParameters = new HashMap<>();
            for (int i = 0; i < STREAMS; i++) {
                UUID uuid = UUID.randomUUID();
                conflictParameters.put(uuid, conflictSetParameters);
            }

            return new TxResolutionInfo(UUID.randomUUID(),
                    new Token(Layout.INVALID_EPOCH, counter.getAndIncrement()),
                    conflictParameters,
                    conflictParameters);
        }
    }

    /**
     * Measure transactional token query performance.
     */
    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    @Threads(5)
    public void txToken(TransactionalSequencerBenchmarkState state, Blackhole bh) {
        TxResolutionInfo resolutionInfo = state.getResolutionInfo();
        Set<UUID> streamSet = resolutionInfo.getConflictSet().keySet();
        bh.consume(state.rt.getSequencerView().next(resolutionInfo, streamSet.toArray(new UUID[streamSet.size()])));
    }
}
