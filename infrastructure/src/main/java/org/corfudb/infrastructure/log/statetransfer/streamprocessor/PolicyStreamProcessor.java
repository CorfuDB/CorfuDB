package org.corfudb.infrastructure.log.statetransfer.streamprocessor;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import org.corfudb.common.result.Result;
import org.corfudb.common.streamutils.StreamUtils;
import org.corfudb.common.streamutils.StreamUtils.StreamHeadAndTail;
import org.corfudb.common.tailcall.TailCall;
import org.corfudb.common.tailcall.TailCalls;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResultData;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFailure;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy.DynamicPolicyData;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy.StaticPolicyData;
import org.corfudb.util.CFUtils;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Builder
public class PolicyStreamProcessor {

    @Default
    private final PolicyStreamProcessorData policyData =
            PolicyStreamProcessorData.builder().build();

    @NonNull
    private final StateTransferBatchProcessor batchProcessor;

    /**
     * Update the current window without sliding it.
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


    private CompletableFuture<Result<Long, StreamProcessFailure>> finalizeSuccessfulTransfers
            (SlidingWindow slidingWindow, CompletableFuture<List<BatchResult>> successful) {
        return slidingWindow.getTotalAddressesTransferred().thenCompose(total ->
                successful.thenApply(s -> Result.ok(s.size() + total)
                        .mapError(e -> new StreamProcessFailure())));
    }

    private CompletableFuture<Result<Long, StreamProcessFailure>> finalizeFailedTransfers
            (CompletableFuture<List<BatchResult>> failed) {
        return failed.thenApply(f -> {
            if (f.isEmpty()) {
                return Result.ok(0L).mapError(e -> new StreamProcessFailure());
            } else {
                return Result.error(new StreamProcessFailure(f.get(0).getResult().getError()));
            }
        });
    }

    private CompletableFuture<Result<Long, StreamProcessFailure>> finalizePendingTransfers
            (CompletableFuture<List<BatchResult>> pending) {

        return pending.thenApply(list -> {
            if (!list.isEmpty()) {
                SimpleEntry<Result<BatchResultData, BatchProcessorFailure>, Long> res = list.stream()
                        .map(result -> new SimpleEntry<>(result.getResult(), 1L))
                        .reduce((x, y) -> {
                            Result<BatchResultData, BatchProcessorFailure> first = x.getKey();
                            Result<BatchResultData, BatchProcessorFailure> second = y.getKey();
                            if (first.isValue() && second.isValue()) {
                                return new SimpleEntry<>(first, 1L + 1L);

                            } else if (first.isValue()) {
                                return new SimpleEntry<>(second, 1L);
                            } else {
                                return new SimpleEntry<>(first, 1L);
                            }
                        }).get();

                Result<BatchResultData, BatchProcessorFailure> accumResult = res.getKey();
                long totalTransferred = res.getValue();
                if (accumResult.isError()) {
                    return Result.error(new StreamProcessFailure(accumResult.getError()));
                } else {
                    return Result.ok(totalTransferred).mapError(e -> new StreamProcessFailure());
                }

            } else {
                return Result.ok(0L).mapError(e -> new StreamProcessFailure());
            }

        });
    }

    private CompletableFuture<Result<Long, StreamProcessFailure>> finalizeTransfers(SlidingWindow slidingWindow) {
        CompletableFuture<Result<Long, StreamProcessFailure>> successfulResult =
                finalizeSuccessfulTransfers(slidingWindow, CFUtils.sequence(slidingWindow.getProcessed()));

        CompletableFuture<Result<Long, StreamProcessFailure>> failedResult =
                finalizeFailedTransfers(CFUtils.sequence(slidingWindow.getFailed()));

        CompletableFuture<Result<Long, StreamProcessFailure>> pendingResult =
                finalizePendingTransfers(CFUtils.sequence(slidingWindow.getWindow()));

        CompletableFuture<List<Result<Long, StreamProcessFailure>>> allResults =
                CFUtils.sequence(Arrays.asList(successfulResult, failedResult, pendingResult));

        return allResults.thenApply(list -> list.stream().reduce((first, second) ->
                first.flatMap(f -> second.map(s -> f + s))).get());

    }

    private DynamicPolicyData invokeDynamicPolicies(DynamicPolicyData dynamicPolicyData) {
        return policyData.getDynamicDistributionPolicy()
                .applyPolicy(policyData.getBatchProcessorFailureHandlingPolicy()
                        .applyPolicy(dynamicPolicyData))
                .resetDynamicWindow();
    }

    private TailCall<CompletableFuture<Result<Long, StreamProcessFailure>>>
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
            CompletableFuture<BatchResult> transferResult =
                    batchProcessor.transfer(currentBatch);

            return TailCalls.call(() ->
                    doProcessStream(
                            tail,
                            updateWindow(transferResult, slidingWindow)));

        }
        // Head is present, window is full.
        else if (head.isPresent() && currentWindowSize == slidingWindow.getWindowSize()) {
            final Batch currentBatch = head.get();
            CompletableFuture<BatchResult> transferResult = batchProcessor.transfer(currentBatch);
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
        } else {
            return TailCalls.done(finalizeTransfers(slidingWindow));
        }
    }

    public CompletableFuture<Result<Long, StreamProcessFailure>> processStream(StaticPolicyData staticPolicyData) {
        Stream<Optional<Batch>> initStream = policyData.getInitialDistributionPolicy()
                .applyPolicy(staticPolicyData);
        SlidingWindow slidingWindow = SlidingWindow.builder().build();
        return doProcessStream(initStream, slidingWindow)
                .invoke()
                .get();
    }


}
