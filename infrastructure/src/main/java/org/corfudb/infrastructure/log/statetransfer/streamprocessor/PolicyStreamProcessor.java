package org.corfudb.infrastructure.log.statetransfer.streamprocessor;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import org.corfudb.common.streamutils.StreamUtils;
import org.corfudb.common.streamutils.StreamUtils.StreamHeadAndTail;
import org.corfudb.common.tailcall.TailCall;
import org.corfudb.common.tailcall.TailCalls;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.batchpolicy.BatchProcessingPolicy;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.batchpolicy.BatchProcessingPolicyData;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy.DynamicPolicy;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy.DynamicPolicyData;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy.IdentityPolicy;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.errorpolicy.RemoveServersWithRoundRobin;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy.RoundRobinPolicy;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy.StaticPolicy;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy.StaticPolicyData;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Builder
public class PolicyStreamProcessor {

    @Default
    private final StaticPolicy initialDistributionPolicy = new RoundRobinPolicy();

    @Default
    private final DynamicPolicy slidingWindowPolicy = new IdentityPolicy();

    @Default
    private final DynamicPolicy dynamicDistributionPolicy = new IdentityPolicy();

    @Default
    private final DynamicPolicy stateTransferFailureHandlingPolicy = new RemoveServersWithRoundRobin();

    @NonNull
    private final BatchProcessingPolicy batchProcessingPolicy;

    /**
     * Update the current sliding window without sliding it.
     *
     * @param newBatchResult A future of a batch transfer result.
     * @param currentWindow  A current sliding window.
     * @return A new updated window.
     */
    private SlidingWindow updateWindow(CompletableFuture<BatchResult> newBatchResult,
                                       SlidingWindow currentWindow) {
        return currentWindow.toBuilder()
                .totalAddressesTransferred(currentWindow.getTotalAddressesTransferred()
                        .thenApply(x -> x + 1))
                .window(new ImmutableList.Builder<CompletableFuture<BatchResult>>()
                        .addAll(currentWindow.getWindow())
                        .add(newBatchResult).build())
                .build();
    }

    private TailCall<SlidingWindow> slideWindow(CompletableFuture<BatchResult> newBatchResult,
                                                SlidingWindow slidingWindow) {
        if (slidingWindow.canSlideWindow()) {
            CompletableFuture<BatchResult> completedBatch = slidingWindow.getWindow().get(0);

            ImmutableList<CompletableFuture<BatchResult>> newWindow = ImmutableList.copyOf(Stream
                    .concat(slidingWindow.getWindow().stream().skip(1),
                            Stream.of(newBatchResult)).collect(Collectors.toList()));

            return TailCalls.done(completedBatch.thenApply(batch -> {
                if (batch.getResult().isError()) {
                    return slidingWindow.toBuilder()
                            .window(newWindow)
                            .failed(
                                    new ImmutableList
                                            .Builder<CompletableFuture<BatchResult>>()
                                            .addAll(slidingWindow.getFailed())
                                            .add(CompletableFuture.completedFuture(batch))
                                            .build()
                            ).build();
                } else {
                    return slidingWindow.toBuilder()
                            .window(newWindow)
                            .totalAddressesTransferred(slidingWindow.getTotalAddressesTransferred()
                                    .thenApply(x -> x + 1))
                            .processed(
                                    new ImmutableList
                                            .Builder<CompletableFuture<BatchResult>>()
                                            .addAll(slidingWindow.getProcessed())
                                            .add(CompletableFuture.completedFuture(batch))
                                            .build()
                            ).build();
                }
            }).join());
        } else {
            return TailCalls.call(() -> slideWindow(newBatchResult, slidingWindow));
        }
    }

    private DynamicPolicyData invokeDynamicPolicies(DynamicPolicyData dynamicPolicyData) {
        return dynamicDistributionPolicy
                .applyPolicy(stateTransferFailureHandlingPolicy
                        .applyPolicy(dynamicPolicyData))
                .resetDynamicWindow();
    }

    private TailCall<SlidingWindow>
    doProcessStream(Stream<Optional<Batch>> stream,
                    SlidingWindow slidingWindow) {
        StreamHeadAndTail<Batch> headAndTail = StreamUtils.splitTail(stream);
        Optional<Batch> head = headAndTail.getHead();
        Stream<Optional<Batch>> tail = headAndTail.getTail();
        final int currentWindowSize = slidingWindow.getWindow().size();
        // Head is present and the window is not full yet.
        if (head.isPresent() && currentWindowSize < slidingWindow.getWindowSize()) {
            // Apply current transfer batch policy
            final Batch currentBatch = head.get();
            CompletableFuture<BatchResult> transferResult = batchProcessingPolicy
                    .applyPolicy(new BatchProcessingPolicyData(currentBatch));

            return TailCalls.call(() ->
                    doProcessStream(
                            tail,
                            updateWindow(transferResult, slidingWindow)));

        }
        // Head is present, window is full.
        else if (head.isPresent() && currentWindowSize == slidingWindow.getWindowSize()) {
            final Batch currentBatch = head.get();
            CompletableFuture<BatchResult> transferResult = batchProcessingPolicy
                    .applyPolicy(new BatchProcessingPolicyData(currentBatch));
            SlidingWindow newWindow = slideWindow(transferResult, slidingWindow).invoke()
                    .get();
            // Recursion is guaranteed to execute, since we lift all the errors to
            // result at the lower levels.
            if (newWindow.canInvokeDynamicPolicies()) {
                DynamicPolicyData dynamicPolicyData = invokeDynamicPolicies(new DynamicPolicyData(tail, newWindow));
                return TailCalls.call(() -> doProcessStream(
                        dynamicPolicyData.getTail(),
                        dynamicPolicyData.getSlidingWindow()
                ));
            } else {
                return TailCalls.call(() -> doProcessStream(
                        tail,
                        newWindow
                ));
            }
        }

        // Head is not present, stream termination
        else {
            return TailCalls.done(slidingWindow);
        }
    }

    public CompletableFuture<Long> processStream(StaticPolicyData staticPolicyData) {
        Stream<Optional<Batch>> initStream = initialDistributionPolicy.applyPolicy(staticPolicyData);
        SlidingWindow slidingWindow = SlidingWindow.builder().build();
        return doProcessStream(initStream, slidingWindow)
                .invoke()
                .get() // Guaranteed to execute since the stream is finite.
                .getTotalAddressesTransferred();
    }


}
