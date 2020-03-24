package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.annotations.VisibleForTesting;
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
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;
import org.corfudb.runtime.clients.LogUnitClient;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.TRANSFERRED;

/**
 * A class responsible for managing a state transfer on the current node.
 * It executes the state transfer for each non-transferred segment synchronously.
 */
@Slf4j
@Builder
public class StateTransferManager {

    /**
     * A log unit client to the current node.
     */
    @Getter
    @NonNull
    private final LogUnitClient logUnitClient;

    /**
     * A size of one batch of transfer.
     */
    @Getter
    @NonNull
    private final int batchSize;

    /**
     * A batch processor that transfers addresses one batch at a time.
     */
    @Getter
    @NonNull
    private final StateTransferBatchProcessor batchProcessor;


    /**
     * Given a range, return the addresses that are currently not present in the stream log.
     *
     * @param rangeStart Start address (inclusive).
     * @param rangeEnd   End address (inclusive).
     * @return A list of addresses, currently not present in the stream log.
     */
    @VisibleForTesting
    ImmutableList<Long> getUnknownAddressesInRange(long rangeStart, long rangeEnd) {
        Set<Long> knownAddresses = logUnitClient
                .requestKnownAddresses(rangeStart, rangeEnd).join().getKnownAddresses();

        return LongStream.range(rangeStart, rangeEnd + 1L)
                .filter(address -> !knownAddresses.contains(address))
                .boxed()
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Performs the state transfer for the current transfer segments and also
     * updates their state as a result.
     *
     * @param beforeTransferSegments A list of segments before a transfer.
     * @return A list of segments after a transfer.
     */
    public ImmutableList<TransferSegment> handleTransfer(List<TransferSegment> beforeTransferSegments) {

        List<TransferSegment> afterTransferSegments = new ArrayList<>();

        for (TransferSegment segment : beforeTransferSegments) {
            TransferSegmentStatus newStatus = segment.getStatus();

            // If a segment is not NOT_TRANSFERRED -> return it as is.
            if (newStatus.getSegmentState() != NOT_TRANSFERRED) {
                afterTransferSegments.add(segment);
                continue;
            }

            // Get all the unknown addresses for this segment.
            List<Long> unknownAddressesInRange =
                    getUnknownAddressesInRange(segment.getStartAddress(),
                            segment.getEndAddress());

            // If no addresses to transfer - mark a segment as transferred.
            if (unknownAddressesInRange.isEmpty()) {
                log.debug("All addresses are present in the range: [{}, {}], skipping transfer.",
                        segment.getStartAddress(), segment.getEndAddress());
                long totalTransferred = segment.getTotal();

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
                        .map(groupedAddresses ->
                                new TransferBatchRequest(groupedAddresses, Optional.empty())
                        );
                // Execute state transfer synchronously.
                newStatus = synchronousStateTransfer(batchStream, numAddressesToTransfer);
            }

            TransferSegment currentSegment = TransferSegment
                    .builder()
                    .startAddress(segment.getStartAddress())
                    .endAddress(segment.getEndAddress())
                    .status(newStatus)
                    .build();

            afterTransferSegments.add(currentSegment);

            if (currentSegment.getStatus().getSegmentState() == FAILED) {
                // A segment failed -> short circuit.
                break;
            }
        }
        return ImmutableList.copyOf(afterTransferSegments);
    }

    /**
     * Given a stream of batch requests, and a total number
     * of transferred addresses needed, execute a state transfer synchronously.
     *
     * @param batchStream A stream of batch requests.
     * @param totalNeeded A total number of addresses needed for transfer.
     * @return A status representing a final status of a transferred segment.
     */
    @VisibleForTesting
    TransferSegmentStatus synchronousStateTransfer(
            Stream<TransferBatchRequest> batchStream, long totalNeeded) {
        long accTransferred = 0L;

        Iterator<TransferBatchRequest> iterator = batchStream.iterator();

        while (iterator.hasNext()) {
            TransferBatchRequest nextBatch = iterator.next();
            TransferBatchResponse response =
                    batchProcessor.transfer(nextBatch).join();
            // In case of an error that is not handled by a batch processor, e.g. WrongEpochException,
            // we return a failed segment status to the caller and the exception. This exception is
            // thrown in a retry block of a RestoreRedundancyMergeSegments' restoreWithBackOff
            // method, which results in a retry.
            // The layout will be invalidated and only the non-transferred addresses of the
            // segment will be considered for the subsequent transfer.
            if (response.getStatus() == TransferStatus.FAILED) {
                Optional<TransferSegmentException> causeOfFailure =
                        Optional.of(response.getCauseOfFailure()
                                .map(TransferSegmentException::new)
                                .orElse(new TransferSegmentException("Failed to transfer.")));

                return TransferSegmentStatus
                        .builder()
                        .totalTransferred(accTransferred)
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

        String errorMsg = String.format("Needed: %s, but transferred: %s",
                totalNeeded, accTransferred);

        return TransferSegmentStatus
                .builder()
                .totalTransferred(0L)
                .segmentState(FAILED)
                .causeOfFailure(Optional.of(new TransferSegmentException(errorMsg)))
                .build();
    }

    /**
     * A data class that represents a non-empty and bounded segment to be transferred.
     */
    @EqualsAndHashCode
    @Getter
    @ToString
    @Builder
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class TransferSegment {
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

        /**
         * Get the total number of addresses in range.
         * {@link #endAddress} and {@link #startAddress} can only be non-negative longs such that
         * {@link #endAddress} >= {@link #startAddress}.
         *
         * @return Total number of addresses in this segment.
         */
        public long getTotal() {
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
}
