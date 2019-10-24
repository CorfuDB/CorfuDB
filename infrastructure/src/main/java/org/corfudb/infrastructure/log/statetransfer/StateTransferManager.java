package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessorData;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.StreamProcessFailure;
import org.corfudb.util.CFUtils;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.corfudb.runtime.view.Address.NON_ADDRESS;

/**
 * A class responsible for managing a state transfer on the current node.
 * It executes the state transfer for each non-transferred segment synchronously.
 */
@Slf4j
@AllArgsConstructor
public class StateTransferManager {

    /**
     * States of the segment:
     * - {@link SegmentState#NOT_TRANSFERRED}: Segment is not transferred.
     * - {@link SegmentState#TRANSFERRED}: Segment was transferred fully.
     * - {@link SegmentState#RESTORED}: Segment was restored, and is present in the current layout.
     * - {@link SegmentState#FAILED}: The state transfer of a segment has failed.
     */
    public enum SegmentState {
        NOT_TRANSFERRED,
        TRANSFERRED,
        RESTORED,
        FAILED
    }

    /**
     * A data class that represent one segment to be transferred.
     */
    @EqualsAndHashCode(exclude = "status")
    @Getter
    @ToString
    @Builder
    public static class CurrentTransferSegment {
        /**
         * Start address of a segment.
         */
        @Default
        private final long startAddress = NON_ADDRESS;
        /**
         * End address of a segment.
         */
        @Default
        private final long endAddress = NON_ADDRESS;
        /**
         * A future that hold the status of a transfer of a segment.
         */
        @Default
        @NonNull
        private final CompletableFuture<CurrentTransferSegmentStatus> status;

        public static class CurrentTransferSegmentBuilder {
            private CompletableFuture<CurrentTransferSegmentStatus> status;

            /**
             * Pass the status and wrap it into the completable future.
             *
             * @param segmentStatus Segment status.
             * @return Completed future of a segment status.
             */
            public CurrentTransferSegmentBuilder completedStatus(
                    CurrentTransferSegmentStatus segmentStatus
            ) {
                this.status = CompletableFuture.completedFuture(segmentStatus);
                return this;
            }

            /**
             * Pass the future of a status.
             *
             * @param segmentStatus A future of a status.
             * @return A future of a status.
             */
            public CurrentTransferSegmentBuilder status(
                    CompletableFuture<CurrentTransferSegmentStatus> segmentStatus
            ) {
                this.status = segmentStatus;
                return this;
            }
        }

        /**
         * If end address and start address are valid, compute the total number of transferred.
         *
         * @return Sum of total transferred.
         */
        public long computeTotalTransferred() {
            return endAddress - startAddress + 1L;
        }
    }

    /**
     * A data class that represent a status of a segment to be transferred.
     */
    @Getter
    @ToString
    @EqualsAndHashCode(exclude = "causeOfFailure")
    @Builder
    public static class CurrentTransferSegmentStatus {
        /**
         * A state of a segment.
         */
        @Default
        private final SegmentState segmentState = RESTORED;
        /**
         * Total number of records transferred for this segment.
         */
        @Default
        private final long totalTransferred = 0L;
        /**
         * An optional cause of failure for this segment.
         */
        @Default
        private final Optional<StreamProcessFailure> causeOfFailure = Optional.empty();
    }

    @Getter
    @NonNull
    private final StreamLog streamLog;

    @Getter
    @NonNull
    private final int batchSize;

    /**
     * Given a range, return the addresses that are currently not present in the stream log.
     *
     * @param rangeStart Start address.
     * @param rangeEnd   End address.
     * @return A list of addresses, currently not present in the stream log.
     */
    ImmutableList<Long> getUnknownAddressesInRange(long rangeStart, long rangeEnd) {
        Set<Long> knownAddresses = streamLog
                .getKnownAddressesInRange(rangeStart, rangeEnd);

        return LongStream.range(rangeStart, rangeEnd + 1L)
                .filter(address -> !knownAddresses.contains(address))
                .boxed()
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Performs the state transfer for the current transfer segments and also
     * updates their state as a result.
     *
     * @param currentTransferSegments A list of the segment currently present in the system.
     * @param batchProcessor          An instance of a batch processor.
     * @return A list with the updated transfer segments.
     */
    public ImmutableList<CurrentTransferSegment> handleTransfer
    (List<CurrentTransferSegment> currentTransferSegments,
     StateTransferBatchProcessor batchProcessor
    ) {
        return currentTransferSegments.stream().map(segment ->
                {   // For each of the segments:
                    CompletableFuture<CurrentTransferSegmentStatus> newStatus = segment
                            .getStatus()
                            .thenCompose(status -> {
                                // If a segment is not transferred -> transfer.
                                if (status.getSegmentState().equals(NOT_TRANSFERRED)) {
                                    // Get all the unknown addresses for this segment.
                                    List<Long> unknownAddressesInRange =
                                            getUnknownAddressesInRange(segment.getStartAddress(),
                                                    segment.getEndAddress());

                                    // If no addresses to transfer - mark a segment as transferred.
                                    if (unknownAddressesInRange.isEmpty()) {
                                        long totalTransferred = segment.computeTotalTransferred();

                                        return CompletableFuture.completedFuture(
                                                CurrentTransferSegmentStatus
                                                        .builder()
                                                        .segmentState(TRANSFERRED)
                                                        .totalTransferred(totalTransferred)
                                                        .build());
                                    } else {
                                        // Get total number of addresses needed to transfer.
                                        long numAddressesToTransfer = unknownAddressesInRange.size();
                                        // Create a batch stream.
                                        Stream<Batch> batchStream = Lists
                                                .partition(unknownAddressesInRange, batchSize)
                                                .stream()
                                                .map(groupedAddresses ->
                                                        new Batch(groupedAddresses, Optional.empty()
                                                        )
                                                );
                                        // Execute state transfer synchronously.
                                        return synchronousStateTransfer(batchProcessor, batchStream)
                                                .thenApply(result ->
                                                        createStatusBasedOnTransferResult(result,
                                                                numAddressesToTransfer));

                                    }
                                    // If a segment contains some other status -> return.
                                } else {
                                    return CompletableFuture.completedFuture(status);
                                }
                            });
                    return CurrentTransferSegment
                            .builder()
                            .startAddress(segment.getStartAddress())
                            .endAddress(segment.getEndAddress())
                            .status(newStatus)
                            .build();
                }


        ).collect(ImmutableList.toImmutableList());
    }

    /**
     * Given a batch processor and a stream of batches, execute the state transfer.
     *
     * @param batchProcessor An instance of a batch processor.
     * @param batchStream    A stream of batches.
     * @return A completed future containing a result of total number of addresses transferred
     * or an exception.
     */
    CompletableFuture<Result<Long, StreamProcessFailure>> synchronousStateTransfer(
            StateTransferBatchProcessor batchProcessor, Stream<Batch> batchStream) {
        Result<Long, StreamProcessFailure> accumulatedResult = Result.ok(0L);

        Result<Long, StreamProcessFailure> resultOfTransfer =
                batchStream.reduce(accumulatedResult, (resultSoFar, nextBatch) -> {
                    BatchResult batchResult = CFUtils
                            .getUninterruptibly(batchProcessor.transfer(nextBatch));
                    if (batchResult.getStatus() == BatchResult.FailureStatus.FAILED) {
                        return Result.error(new StreamProcessFailure());
                    } else {
                        return resultSoFar
                                .map(sumSoFar -> sumSoFar + batchResult.getAddresses().size());
                    }
                }, (oldSum, newSum) -> newSum);

        return CompletableFuture.completedFuture(resultOfTransfer);
    }


    /**
     * Based on a result of a state transfer, update a segment status.
     *
     * @param result      A result, containing a total number of records transferred or a failure.
     * @param totalNeeded A total number of records that must be transferred.
     * @return An updated segment status.
     */
    CurrentTransferSegmentStatus createStatusBasedOnTransferResult(Result<Long,
            StreamProcessFailure> result, long totalNeeded) {
        Result<Long, StreamProcessFailure> checkedResult =
                result.flatMap(totalTransferred -> {
                    if (totalTransferred != totalNeeded) {
                        return Result.error(new StreamProcessFailure
                                (new IllegalStateException
                                        ("Needed " + totalNeeded +
                                                " but transferred " + totalTransferred)));
                    } else {
                        return Result.ok(totalTransferred);
                    }
                });

        if (checkedResult.isError()) {
            return CurrentTransferSegmentStatus
                    .builder()
                    .totalTransferred(0L)
                    .segmentState(FAILED)
                    .causeOfFailure(Optional.of(checkedResult.getError()))
                    .build();
        } else {
            return CurrentTransferSegmentStatus
                    .builder()
                    .totalTransferred(checkedResult.get())
                    .segmentState(TRANSFERRED)
                    .build();
        }
    }

}