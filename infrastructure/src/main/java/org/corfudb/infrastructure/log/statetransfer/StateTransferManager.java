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
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.*;

/**
 * A class responsible for managing a state transfer on the current node.
 * It executes the state transfer for each non-transferred segment synchronously.
 */
@Slf4j
@AllArgsConstructor
public class StateTransferManager {

    /**
     * A data class that represents a non-empty and bounded segment to be transferred.
     */
    @EqualsAndHashCode
    @Getter
    @ToString
    @Builder
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class TransferSegment implements Comparable<TransferSegment> {
        /**
         * Start address of a segment range to transfer, inclusive and non-negative.
         */
        private final long startAddress;
        /**
         * End address of a segment range to transfer, inclusive and non-negative.
         */
        private final long endAddress;
        /**
         * A status of a transfer of a segment.
         */
        private final TransferSegmentStatus status;

        @Override
        public int compareTo(TransferSegment other) {
            return Long.compare(this.getStartAddress(), other.getStartAddress());
        }


        /**
         * Whether a current transfer segment overlaps with another transfer segment.
         *
         * @param other Other segment.
         * @return True, if overlap.
         */
        public boolean overlapsWith(TransferSegment other) {
            return Optional.ofNullable(other)
                    .map(otherSegment -> otherSegment.getStartAddress() <= this.getEndAddress())
                    .orElse(false);
        }


        /**
         * Compute the total number of transferred addresses.
         * {@link #endAddress} and {@link #startAddress} can only be non-negative longs such that
         * {@link #endAddress} >= {@link #startAddress}.
         *
         * @return Sum of the total addresses transferred.
         */
        public long computeTotalTransferred() {
            return endAddress - startAddress + 1L;
        }

        public static class TransferSegmentBuilder {

            public void verify() {
                if (startAddress < 0L || endAddress < 0L) {
                    throw new IllegalStateException(
                            String.format("Start: %s or end: %s " +
                                    "can not be negative.", startAddress, endAddress));
                }
                if (startAddress > endAddress) {
                    throw new IllegalStateException(
                            String.format("Start: %s can not be " +
                                    "greater than end: %s.", startAddress, endAddress));
                }

                if (status == null) {
                    throw new IllegalStateException("Status should be defined.");
                }
            }

            public TransferSegment build() {
                verify();
                return new TransferSegment(startAddress, endAddress, status);
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
    public static class TransferSegmentStatus {
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
        private final Optional<TransferSegmentException> causeOfFailure = Optional.empty();
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
     * @param transferSegments A list of the segment currently present in the system.
     * @param batchProcessor   An instance of a batch processor.
     * @return A list with the updated transfer segments.
     */
    public ImmutableList<TransferSegment> handleTransfer(
            List<TransferSegment> transferSegments, StateTransferBatchProcessor batchProcessor) {

        return transferSegments.stream().map(segment -> {   // For each of the segments:
                    TransferSegmentStatus newStatus = segment.getStatus();

                    // If a segment is not NOT_TRANSFERRED -> return it as is.
                    if (newStatus.getSegmentState() != NOT_TRANSFERRED) {
                        return segment;
                    }

                    // Get all the unknown addresses for this segment.
                    List<Long> unknownAddressesInRange =
                            getUnknownAddressesInRange(segment.getStartAddress(),
                                    segment.getEndAddress());

                    // If no addresses to transfer - mark a segment as transferred.
                    if (unknownAddressesInRange.isEmpty()) {
                        long totalTransferred = segment.computeTotalTransferred();

                        newStatus = TransferSegmentStatus
                                .builder()
                                .segmentState(TRANSFERRED)
                                .totalTransferred(totalTransferred)
                                .build();
                    } else {
                        // Get total number of addresses needed to transfer.
                        long numAddressesToTransfer = unknownAddressesInRange.size();
                        // Create a transferBatchRequest stream.
                        Stream<TransferBatchRequest> batchStream = Lists
                                .partition(unknownAddressesInRange, batchSize)
                                .stream()
                                .map(groupedAddresses -> new TransferBatchRequest(groupedAddresses, Optional.empty())
                                );
                        // Execute state transfer synchronously.
                        newStatus = synchronousStateTransfer(batchProcessor, batchStream, numAddressesToTransfer);
                    }
                    // If a segment contains some other status -> return.
                    return TransferSegment
                            .builder()
                            .startAddress(segment.getStartAddress())
                            .endAddress(segment.getEndAddress())
                            .status(newStatus)
                            .build();
                }

        ).collect(ImmutableList.toImmutableList());
    }

    /**
     * Given a batch processor, a stream of batch requests, and a total number
     * of transferred addresses needed, execute a state transfer synchronously.
     *
     * @param batchProcessor An instance of a batch processor.
     * @param batchStream    A stream of batch requests.
     * @param totalNeeded    A total number of addresses needed for transfer.
     * @return A status representing a final status of a transferred segment.
     */
    TransferSegmentStatus synchronousStateTransfer(
            StateTransferBatchProcessor batchProcessor,
            Stream<TransferBatchRequest> batchStream,
            long totalNeeded) {
        long accTransferred = 0L;

        Iterator<TransferBatchRequest> iterator = batchStream.iterator();

        while (iterator.hasNext()) {
            TransferBatchRequest nextBatch = iterator.next();
            TransferBatchResponse response =
                    batchProcessor.transfer(nextBatch).join();

            if (response.getStatus() == TransferStatus.FAILED) {
                Optional<TransferSegmentException> causeOfFailure =
                        Optional.of(new TransferSegmentException("Failed to transfer a batch."));
                return TransferSegmentStatus
                        .builder()
                        .totalTransferred(0L)
                        .segmentState(FAILED)
                        .causeOfFailure(causeOfFailure)
                        .build();
            }
            accTransferred += response.getTransferBatchRequest().getAddresses().size();
        }

        if (accTransferred == totalNeeded) {
            return TransferSegmentStatus
                    .builder()
                    .totalTransferred(accTransferred)
                    .segmentState(TRANSFERRED)
                    .build();
        }

        String errorMsg = "Needed: " + totalNeeded + ", but transferred: " + accTransferred;

        return TransferSegmentStatus
                .builder()
                .totalTransferred(0L)
                .segmentState(FAILED)
                .causeOfFailure(Optional.of(new TransferSegmentException(errorMsg)))
                .build();
    }
}
