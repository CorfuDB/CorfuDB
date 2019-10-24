package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.errorpolicy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFailure;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessor.SlidingWindow;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy.DynamicPolicy;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy.DynamicPolicyData;
import org.corfudb.util.CFUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A failure policy that reschedules the recently failed batch transfers
 * and all the future batch transfers
 * to run on the alive servers in the round robin fashion.
 * If there are no alive servers left, then all
 * the remaining batches fail and the tail of the stream becomes empty.
 */
public class RemoveServersWithRoundRobin implements DynamicPolicy {
    @Override
    public DynamicPolicyData applyPolicy(DynamicPolicyData data) {

        SlidingWindow slidingWindow = data.getSlidingWindow();

        Stream<Optional<Batch>> tail = data.getTail();

        Set<String> allServersSet = new HashSet<>(slidingWindow.getAllServers());

        ImmutableList<CompletableFuture<BatchResult>> failed = slidingWindow.getFailed();

        List<BatchResult> failedBatches = CFUtils.sequence(failed).join();

        Set<String> failedServersSet = failedBatches.stream()
                .map(BatchResult::getDestinationServer)
                .filter(Optional::isPresent)
                .map(Optional::get).collect(Collectors.toSet());

        Set<String> remainingServersSet = Sets.difference(allServersSet, failedServersSet);

        ImmutableList<String> remainingServers = ImmutableList.copyOf(remainingServersSet);

        if (remainingServers.isEmpty()) {
            Stream<Optional<Batch>> newTail = Stream.empty();
            ImmutableList<CompletableFuture<BatchResult>> failedTail =
                    tail.filter(Optional::isPresent)
                            .map(batch ->
                                    CompletableFuture.completedFuture(
                                            BatchResult
                                                    .builder()
                                                    .status(BatchResult.FailureStatus.FAILED)
                                                    .build()))
                            .collect(ImmutableList.toImmutableList());
            SlidingWindow newSlidingWindow = slidingWindow.toBuilder()
                    .allServers(remainingServers)
                    .failed(new ImmutableList
                            .Builder<CompletableFuture<BatchResult>>().addAll(failed)
                            .addAll(failedTail).build())
                    .build();

            return new DynamicPolicyData(newTail, newSlidingWindow, 0L);

        } else {
            // Update the current servers & a failed list.
            SlidingWindow newSlidingWindow = slidingWindow.toBuilder()
                    .allServers(remainingServers)
                    .failed(ImmutableList.of())
                    .build();

            // Reschedule failed batches and the tail on the remaining servers
            // with the round robin policy.
            int failedBatchesSize = failedBatches.size();
            Stream<Optional<Batch>> rescheduledBatches = failedBatches.stream()
                    .map(failedBatch -> Optional.of(failedBatch.toBatch()));

            AtomicInteger count = new AtomicInteger(0);

            Stream<Optional<Batch>> newTail = Stream
                    .concat(rescheduledBatches, tail)
                    .map(optBatch ->
                            optBatch.map(batch -> {
                                String scheduledNode = remainingServers
                                        .get(count.getAndIncrement() % remainingServers.size());
                                return new Batch(batch.getAddresses(), Optional.of(scheduledNode));
                            }));

            return new DynamicPolicyData(
                    newTail,
                    newSlidingWindow,
                    data.getSize() + failedBatchesSize);
        }

    }
}
