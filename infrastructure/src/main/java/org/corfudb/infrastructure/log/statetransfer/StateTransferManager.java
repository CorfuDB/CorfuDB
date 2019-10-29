package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Exclude;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.TransferSegmentFailure;

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
    @EqualsAndHashCode
    @Getter
    @ToString
    @Builder
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class CurrentTransferSegment implements Comparable<CurrentTransferSegment> {
        /**
         * Start address of a segment.
         */
        private final long startAddress;
        /**
         * End address of a segment.
         */
        private final long endAddress;
        /**
         * A future that holds the status of a transfer of a segment.
         */
        private final CurrentTransferSegmentStatus status;

        @Override
        public int compareTo(CurrentTransferSegment other) {
            return Long.compare(this.getStartAddress(), other.getStartAddress());
        }


        /**
         * Whether a current segment overlaps with another segment.
         *
         * @param other Other segment.
         * @return True, if overlap.
         */
        public boolean overlapsWith(CurrentTransferSegment other) {
            return other.getStartAddress() <= this.getEndAddress();
        }


        /**
         * If end address and start address are valid, compute the total number of transferred.
         * Otherwise, if an end address is -1L -> Nothing to compute, return 0L.
         *
         * @return Sum of total transferred.
         */
        public long computeTotalTransferred() {
            if (endAddress == -1L) {
                return 0L;
            }
            return endAddress - startAddress + 1L;
        }


        public static class CurrentTransferSegmentBuilder {

            public void verify() {
                if (startAddress < 0L) {
                    throw new IllegalStateException(
                            String.format("Start %s can not be negative.", startAddress));
                }

                if (status == null) {
                    throw new IllegalStateException("Status should be defined.");
                }
            }

            public CurrentTransferSegment build() {
                verify();
                return new CurrentTransferSegment(startAddress, endAddress, status);
            }
        }
    }

    /**
     * A data class that represents a status of a segment to be transferred.
     */
    @Getter
    @ToString
    @EqualsAndHashCode
    @Builder
    public static class CurrentTransferSegmentStatus {
        /**
         * A state of a segment.
         */
        @Default
        private final SegmentState segmentState = NOT_TRANSFERRED;
        /**
         * Total number of records transferred for this segment.
         */
        @Default
        private final long totalTransferred = 0L;
        /**
         * An optional cause of failure for this segment.
         */
        @Default
        @Exclude
        private final Optional<TransferSegmentFailure> causeOfFailure = Optional.empty();
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
     StateTransferBatchProcessor batchProcessor) {

        return currentTransferSegments.stream().map(segment -> {   // For each of the segments:
                    CurrentTransferSegmentStatus newStatus = segment.getStatus();

                    // If a segment is not transferred -> transfer.
                    if (newStatus.getSegmentState() != NOT_TRANSFERRED) {
                        return CurrentTransferSegment
                                .builder()
                                .startAddress(segment.getStartAddress())
                                .endAddress(segment.getEndAddress())
                                .status(newStatus)
                                .build();
                    }


                    // Get all the unknown addresses for this segment.
                    List<Long> unknownAddressesInRange =
                            getUnknownAddressesInRange(segment.getStartAddress(),
                                    segment.getEndAddress());

                    // If no addresses to transfer - mark a segment as transferred.
                    if (unknownAddressesInRange.isEmpty()) {
                        long totalTransferred = segment.computeTotalTransferred();

                        newStatus =
                                CurrentTransferSegmentStatus
                                        .builder()
                                        .segmentState(TRANSFERRED)
                                        .totalTransferred(totalTransferred)
                                        .build();
                    } else {
                        // Get total number of addresses needed to transfer.
                        long numAddressesToTransfer = unknownAddressesInRange.size();
                        // Create a batch stream.
                        Stream<Batch> batchStream = Lists
                                .partition(unknownAddressesInRange, batchSize)
                                .stream()
                                .map(groupedAddresses -> new Batch(groupedAddresses, Optional.empty())
                                );
                        // Execute state transfer synchronously.
                        newStatus = synchronousStateTransfer(batchProcessor, batchStream)
                                .thenApply(result -> createStatusBasedOnTransferResult(result, numAddressesToTransfer))
                                .join();

                    }
                    // If a segment contains some other status -> return.

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
    CompletableFuture<Result<Long, TransferSegmentFailure>> synchronousStateTransfer(
            StateTransferBatchProcessor batchProcessor, Stream<Batch> batchStream) {
        Result<Long, TransferSegmentFailure> accumulatedResult = Result.ok(0L);

        Result<Long, TransferSegmentFailure> resultOfTransfer =
                batchStream.reduce(accumulatedResult, (resultSoFar, nextBatch) -> {
                    BatchResult batchResult = batchProcessor.transfer(nextBatch).join();
                    if (batchResult.getStatus() == BatchResult.FailureStatus.FAILED) {
                        return Result.error(new TransferSegmentFailure());
                    } else {
                        return resultSoFar
                                .map(sumSoFar -> sumSoFar + batchResult.getBatch().getAddresses()
                                        .size());
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
    CurrentTransferSegmentStatus createStatusBasedOnTransferResult(
            Result<Long, TransferSegmentFailure> result, long totalNeeded) {

        Result<Long, TransferSegmentFailure> checkedResult = result.flatMap(totalTransferred -> {
            if (totalTransferred != totalNeeded) {
                String error = "Needed: " + totalNeeded + ", but transferred: " + totalTransferred;

                return Result.error(new TransferSegmentFailure(new IllegalStateException(error)));
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
