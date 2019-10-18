package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.errorpolicy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.SlidingWindow;
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

public class RemoveServersWithRoundRobin implements DynamicPolicy {
    @Override
    public DynamicPolicyData applyPolicy(DynamicPolicyData data) {

        SlidingWindow slidingWindow = data.getSlidingWindow();

        Stream<Optional<Batch>> tail = data.getTail();

        Set<String> allServersSet = new HashSet<>(slidingWindow.getAllServers());

        ImmutableList<CompletableFuture<BatchResult>> failed = slidingWindow.getFailed();

        List<BatchResult> failedBatches = CFUtils.sequence(failed).join();

        Set<String> failedServersSet =
                failedBatches.stream()
                        .map(batch -> batch.getResult().getError()
                                .getEndpoint()).collect(Collectors.toSet());

        Set<String> remainingServersSet = Sets.difference(allServersSet, failedServersSet);

        ImmutableList<String> remainingServers = ImmutableList.copyOf(remainingServersSet);

        // Update the current servers & a failed list.
        SlidingWindow newSlidingWindow = slidingWindow.toBuilder()
                .allServers(remainingServers)
                .failed(ImmutableList.of())
                .build();

        // Reschedule failed batches and the tail on the remaining servers
        // with the round robin policy.
        Stream<Optional<Batch>> rescheduledBatches = failedBatches.stream()
                .map(failedBatch -> {
                    List<Long> addresses = failedBatch.getResult().getError().getAddresses();
                    String previousEndpoint = failedBatch.getResult().getError().getEndpoint();

                    return Optional.of(new Batch(addresses, previousEndpoint));
                });

        AtomicInteger count = new AtomicInteger(0);

        Stream<Optional<Batch>> newTail = Stream.concat(rescheduledBatches, tail)
                .map(batch -> batch.map(b -> new Batch(b.getAddresses(), remainingServers
                        .get(count.getAndIncrement() % remainingServers.size()))));


        return new DynamicPolicyData(newTail, newSlidingWindow);
    }
}
