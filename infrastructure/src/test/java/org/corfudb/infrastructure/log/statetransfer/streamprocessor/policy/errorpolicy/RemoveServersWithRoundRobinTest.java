package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.errorpolicy;

import com.google.common.collect.ImmutableList;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFailure;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessor;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessor.SlidingWindow;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.StreamTest;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy.DynamicPolicyData;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class RemoveServersWithRoundRobinTest extends StreamTest {

    // Remaining stream has 3 batches by 5 elements, first one is scheduled on A, second on B, third on C:
    // [(5, A), (5, B), (5, C)]
    // Window got 1 batch in failed list that was scheduled to run on server B that has 3 elements.
    // After the application of a policy a stream should look like this:
    // [(3, A), (5, C), (5, A)), and window should no longer contain the failed batches.

    @Test
    void testRemoveServersDistributeLoad() {
        RemoveServersWithRoundRobin policy = new RemoveServersWithRoundRobin();
        SlidingWindow window = SlidingWindow
                .builder()
                .allServers(ImmutableList.of("A", "B", "C"))
                .failed(ImmutableList.of(CompletableFuture.completedFuture(
                        BatchResult.builder()
                                .status(BatchResult.FailureStatus.FAILED)
                                .addresses(ImmutableList.of(0L, 1L, 2L))
                                .destinationServer(Optional.of("B"))
                                .build()))).build();
        DynamicPolicyData dynamicPolicyData = createDynamicPolicyData(5,
                LongStream.range(3L, 18L).boxed().collect(Collectors.toList()),
                Optional.of(ImmutableList.of("A", "B", "C")),
                window);
        DynamicPolicyData dataAfter = policy.applyPolicy(dynamicPolicyData);
        ImmutableList<Batch> expected = ImmutableList.of(
                new Batch(ImmutableList.of(0L, 1L, 2L), Optional.of("A")),
                new Batch(ImmutableList.of(3L, 4L, 5L, 6L, 7L), Optional.of("C")),
                new Batch(ImmutableList.of(8L, 9L, 10L, 11L, 12L), Optional.of("A")),
                new Batch(ImmutableList.of(13L, 14L, 15L, 16L, 17L), Optional.of("C")));
        assertThat(dataAfter.getTail().map(x -> x.get()).collect(Collectors.toList()))
                .isEqualTo(expected);
        assertThat(dataAfter.getSize()).isEqualTo(4);
        assertThat(dataAfter.getSlidingWindow().getFailed()).isEmpty();

    }

    // Remaining stream has 3 batches by 5 elements, each one is going through a protocol:
    // [(5), (5), (5)]
    // Window got 1 batch in failed list that was scheduled to run but failed even after retries.
    // After the application of a policy a stream should be empty, and a sliding window should have
    // all of the rest of a stream in a failed list

    @Test
    void testRemoveServersCompletely() {
        RemoveServersWithRoundRobin policy = new RemoveServersWithRoundRobin();
        SlidingWindow window = SlidingWindow
                .builder()
                .failed(ImmutableList.of(CompletableFuture.completedFuture(
                        BatchResult.builder()
                                .addresses(ImmutableList.of(0L, 1L, 2L))
                                .status(BatchResult.FailureStatus.FAILED)
                                .build()))).build();

        DynamicPolicyData dynamicPolicyData = createDynamicPolicyData(5,
                LongStream.range(3L, 18L).boxed().collect(Collectors.toList()),
                Optional.empty(),
                window);

        DynamicPolicyData dataAfter = policy.applyPolicy(dynamicPolicyData);
        assertThat(dataAfter.getTail().collect(Collectors.toList())).isEmpty();
        assertThat(dataAfter.getSize()).isEqualTo(0L);
        assertThat(dataAfter.getSlidingWindow().getFailed().isEmpty()).isFalse();
        assertThat(dataAfter.getSlidingWindow()
                .getFailed().size()).isEqualTo(4);


    }
}