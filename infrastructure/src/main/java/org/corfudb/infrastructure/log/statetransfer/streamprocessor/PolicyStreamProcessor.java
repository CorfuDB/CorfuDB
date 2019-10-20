package org.corfudb.infrastructure.log.statetransfer.streamprocessor;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
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

/**
 * A class that handles an actual state transfer for a complete segment or a portion of it.
 * It operates over a lazy stream of batches by running a sliding window algorithm.
 * A sliding window is used to aggregate a dynamic data over the transferred batches
 * and make the timely decisions regarding how the future load will be handled.
 */
@Builder
public class PolicyStreamProcessor {

    /**
     * A sliding window of {@link PolicyStreamProcessor#windowSize} size.
     * It is used to hold the dynamic information needed to complete a state transfer.
     */
    @Getter
    @Builder(toBuilder = true)
    public static class SlidingWindow {

        /**
         * All servers currently available for committed batch processing (not via protocol).
         */
        @Default
        private final ImmutableList<String> allServers = ImmutableList.of();


        /**
         * Total number of addresses transferred so far.
         */
        @Default
        private final CompletableFuture<Long> totalAddressesTransferred =
                CompletableFuture.completedFuture(0L);


        /**
         * A list of batch results that succeeded
         * within the latest {@link SlidingWindow#dynamicProtocolWindowSize}.
         */
        @Default
        private final ImmutableList<CompletableFuture<BatchResult>> succeeded = ImmutableList.of();

        /**
         * A list of batch results that failed
         * within the latest {@link SlidingWindow#dynamicProtocolWindowSize}.
         */
        @Default
        private final ImmutableList<CompletableFuture<BatchResult>> failed = ImmutableList.of();

        /**
         * A current window of the pending batch transfers.
         * Slides when the first scheduled batch transfer completes.
         */
        @Default
        private final ImmutableList<CompletableFuture<BatchResult>> window = ImmutableList.of();

        /**
         * Returns true if the oldest scheduled batch transfer completes.
         *
         * @return True if the window can be slid, false otherwise.
         */
        public boolean canSlideWindow() {
            if (window.isEmpty()) {
                return true;
            } else {
                return window.get(0).isDone();
            }
        }

        /**
         * Returns true if the number of failed and succeeded elements of the current window is equal
         * to {@link SlidingWindow#dynamicProtocolWindowSize}.
         *
         * @return True if the dynamic protocols can be executed and false otherwise.
         */
        public boolean canInvokeDynamicPolicies(int dynamicProtocolWindowSize) {
            return getCurrentDynamicProtocolWindow() == dynamicProtocolWindowSize;
        }

        /**
         * Get a current dynamic protocol window, a number of failed and completed entries.
         *
         * @return The number of failed and completed entries.
         */
        private int getCurrentDynamicProtocolWindow() {
            return failed.size() + succeeded.size();
        }
    }


    /**
     * The static size of a sliding window.
     */
    @NonNull
    private final int windowSize;


    /**
     * The static size of both processed and failed batch transfers after
     * which to invoke the dynamic protocols on the stream.
     */
    @NonNull
    private final int dynamicProtocolWindowSize;


    /**
     * Policies for the sliding window:
     * Initial distribution transfer policy,
     * dynamic distribution transfer policy (invoked every dynamic protocol window size),
     * failure handling policy (invoked every dynamic protocol window size).
     *
     */
    @NonNull
    private final PolicyStreamProcessorData policyData;


    /**
     * A concrete instance of a class that processes one transfer batch, for example:
     * protocol, many-to-one read, file transfer, etc.
     */
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

    /**
     * Slide a current window over a lazy stream of batches.
     * This entails creating a new instance of a window with a new updated data.
     * @param newBatchResult A future of a batch transferred by {@link PolicyStreamProcessor#batchProcessor}.
     * @param slidingWindow The most recent instance of a sliding window.
     * @return An instance of a new sliding window, with a new, updated data,
     * wrapped in a tail-recursive call.
     */
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
                            .succeeded(
                                    new ImmutableList
                                            .Builder<CompletableFuture<BatchResult>>()
                                            .addAll(slidingWindow.getSucceeded())
                                            .add(CompletableFuture.completedFuture(batch))
                                            .build()
                            ).build();
                }
            }).join());
        } else {
            return TailCalls.call(() -> slideWindow(newBatchResult, slidingWindow));
        }
    }

    /**
     * Finalize all the successful transfers of a sliding window.
     * @param totalAddressesTransferred Total addresses transferred.
     * @param successful A list of the latest succeeded batch transfers.
     * @return A result containing a number of addresses transferred or an exception.
     */
    private CompletableFuture<Result<Long, StreamProcessFailure>> finalizeSuccessfulTransfers
            (CompletableFuture<Long> totalAddressesTransferred,
             CompletableFuture<List<BatchResult>> successful) {
        return totalAddressesTransferred.thenCompose(total ->
                successful.thenApply(s -> Result.ok(s.size() + total)
                        .mapError(e -> new StreamProcessFailure())));
    }

    /**
     * Finalize all the failed transfers of a sliding window.
     * @param failed A list of the latest failed batch transfers.
     * @return A result containing a number of addresses transferred or an exception.
     */
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

    /**
     * Finalize all the pending transfers of a sliding window.
     * @param pending A list of the latest pending batch transfers.
     * @return A result containing a number of addresses transferred or an exception.
     */
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

    /**
     * Finalize all the transfers of a sliding window.
     * @param slidingWindow The last instance of a sliding window.
     * @return A result containing a total number of addresses transferred or an exception.
     */
    private CompletableFuture<Result<Long, StreamProcessFailure>> finalizeTransfers(SlidingWindow slidingWindow) {
        CompletableFuture<Result<Long, StreamProcessFailure>> successfulResult =
                finalizeSuccessfulTransfers(slidingWindow.getTotalAddressesTransferred(),
                        CFUtils.sequence(slidingWindow.getSucceeded()));

        CompletableFuture<Result<Long, StreamProcessFailure>> failedResult =
                finalizeFailedTransfers(CFUtils.sequence(slidingWindow.getFailed()));

        CompletableFuture<Result<Long, StreamProcessFailure>> pendingResult =
                finalizePendingTransfers(CFUtils.sequence(slidingWindow.getWindow()));

        CompletableFuture<List<Result<Long, StreamProcessFailure>>> allResults =
                CFUtils.sequence(Arrays.asList(successfulResult, failedResult, pendingResult));

        return allResults.thenApply(list -> list.stream().reduce((first, second) ->
                first.flatMap(f -> second.map(s -> f + s))).get());

    }

    /**
     * Invoke the dynamic policies on the tail of a stream:
     * a distribution policy and failure handling policy.
     * @param dynamicPolicyData Data needed to transform the tail of a stream dynamically.
     * @return A new dynamic policy data: a transformed stream and a new sliding window.
     */
    private DynamicPolicyData invokeDynamicPolicies(DynamicPolicyData dynamicPolicyData) {
        return policyData.getDynamicDistributionPolicy()
                .applyPolicy(policyData.getBatchProcessorFailureHandlingPolicy()
                        .applyPolicy(dynamicPolicyData))
                .resetDynamicWindow();
    }

    /**
     * Perform a state transfer over a stream lazily.
     * @param stream A tail or a non processed part of a stream.
     * @param slidingWindow The latest sliding window.
     * @return A future of a result, containing a total number of records transferred
     * or an exception, wrapped in a tail-recursive call.
     */
    private TailCall<CompletableFuture<Result<Long, StreamProcessFailure>>>
    doProcessStream(Stream<Optional<Batch>> stream,
                    SlidingWindow slidingWindow) {
        // Split the stream into a head and a tail.
        StreamHeadAndTail<Batch> headAndTail = StreamUtils.splitTail(stream);
        Optional<Batch> head = headAndTail.getHead();
        Stream<Optional<Batch>> tail = headAndTail.getTail();
        final int currentWindowSize = slidingWindow.getWindow().size();
        // Head is present and the window is not full yet.
        if (head.isPresent() && currentWindowSize < windowSize) {
            // Transfer the batch with a batch processor instance.
            final Batch currentBatch = head.get();
            CompletableFuture<BatchResult> transferResult =
                    batchProcessor.transfer(currentBatch);

            // Update the window and call this function recursively
            // with a new window and the tail of a stream.
            return TailCalls.call(() ->
                    doProcessStream(
                            tail,
                            updateWindow(transferResult, slidingWindow)));

        }
        // Head is present, window is full.
        else if (head.isPresent() && currentWindowSize == windowSize) {
            // Transfer the batch with a batch processor instance.
            final Batch currentBatch = head.get();
            CompletableFuture<BatchResult> transferResult = batchProcessor.transfer(currentBatch);
            // Slide a window and get a new updated window instance.
            // Recursion is guaranteed to execute, since the batch transfer eventually completes
            // with a result of a value or a propagated exception.
            SlidingWindow newWindow = slideWindow(transferResult, slidingWindow).invoke()
                    .get();
            // If the dynamic policies can be invoked on a tail of a stream, invoke.
            // Then call this function recursively.
            if (newWindow.canInvokeDynamicPolicies(dynamicProtocolWindowSize)) {
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
            // End of a stream, terminate, finalize transfers and return from the function.
        } else {
            return TailCalls.done(finalizeTransfers(slidingWindow));
        }
    }

    /**
     * Execute the state transfer.
     * @param staticPolicyData A data needed to initialize a stream.
     * @return A future of a result, containing a total number of records transferred or an exception. 
     */
    public CompletableFuture<Result<Long, StreamProcessFailure>> processStream(StaticPolicyData staticPolicyData) {
        Stream<Optional<Batch>> initStream = policyData.getInitialDistributionPolicy()
                .applyPolicy(staticPolicyData).map(Optional::of);
        SlidingWindow slidingWindow = SlidingWindow
                .builder().build();
        return doProcessStream(initStream, slidingWindow)
                .invoke()
                .get();
    }


}
