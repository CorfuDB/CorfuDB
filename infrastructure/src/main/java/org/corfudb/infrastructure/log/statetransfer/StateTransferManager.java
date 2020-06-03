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
import org.corfudb.common.util.Tuple;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.UnknownAddressInRangeException;
import org.corfudb.infrastructure.log.statetransfer.transferprocessor.BasicTransferProcessor;
import org.corfudb.infrastructure.log.statetransfer.transferprocessor.ParallelTransferProcessor;
import org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessor.TransferProcessorResult;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.CFUtils;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.StateTransferType.CONSISTENT_READ;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.StateTransferType.PROTOCOL_READ;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest.TransferBatchType.DATA;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_SUCCEEDED;

/**
 * A class responsible for managing a state transfer on the current node.
 */
@Slf4j
@Builder
public class StateTransferManager {


    public enum StateTransferType {
        PROTOCOL_READ,
        CONSISTENT_READ
    }

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
     * A processor that parallelizes the transfer workload.
     */
    @Getter
    @NonNull
    private final ParallelTransferProcessor parallelTransferProcessor;

    /**
     * A processor that transfers the workload via a replication protocol.
     */
    @Getter
    @NonNull
    private final BasicTransferProcessor basicTransferProcessor;

    /**
     * Given a range, return the addresses that are currently not present in the stream log.
     *
     * @param rangeStart Start address (inclusive).
     * @param rangeEnd   End address (inclusive).
     * @return A list of addresses, currently not present in the stream log.
     */
    @VisibleForTesting
    ImmutableList<Long> getUnknownAddressesInRange(long rangeStart, long rangeEnd) {
        try {
            Set<Long> knownAddresses = CFUtils.getUninterruptibly(logUnitClient
                            .requestKnownAddresses(rangeStart, rangeEnd),
                    TimeoutException.class,
                    NetworkException.class,
                    WrongEpochException.class)
                    .getKnownAddresses();

            return LongStream.range(rangeStart, rangeEnd + 1L)
                    .filter(address -> !knownAddresses.contains(address))
                    .boxed()
                    .collect(ImmutableList.toImmutableList());
        } catch (TimeoutException | NetworkException | WrongEpochException e) {
            // This function is called within a lambda expression, which does
            // not allow checked exceptions. So we wrap all the exceptions that can occur
            // here it the UnknownAddressInRangeException.
            throw new UnknownAddressInRangeException(e);
        }
    }

    /**
     * Return a range with the updated list of unknown addresses.
     *
     * @param range A transfer segment range.
     * @return An updated transfer segment range.
     */
    TransferSegmentRangeSingle getUnknownAddressesInRangeForRange(TransferSegmentRangeSingle range) {
        long startAddress = range.getStartAddress();
        long endAddress = range.getEndAddress();
        ImmutableList<Long> unknownAddressesInRange =
                getUnknownAddressesInRange(startAddress, endAddress);
        return range.toBuilder()
                .unknownAddressesInRange(unknownAddressesInRange)
                .build();
    }

    /**
     * Transform the given range into a stream of batch requests.
     *
     * @param range A transfer segment range that contains unknown addresses and maybe available
     *              log unit servers.
     * @return A stream of transfer batch requests.
     */
    Stream<TransferBatchRequest> rangeToBatchRequestStream(TransferSegmentRangeSingle range) {
        ImmutableList<Long> unknownAddressesInRange = range.unknownAddressesInRange;
        Optional<ImmutableList<String>> availableServers = range.availableServers;
        return Lists.partition(unknownAddressesInRange, batchSize).stream()
                .map(partition -> new TransferBatchRequest(partition, availableServers, DATA));
    }

    /**
     * Go over all the single transfer segment ranges, filter them by the provided type,
     * get all the unknown addresses in range, and then turn the unknown addresses in range into
     * the transfer batch request stream - ready to be consumed by the appropriate
     * state transfer processor.
     *
     * @param ranges         A list of single transfer segment ranges.
     * @param typeOfTransfer A provided type of transfer - protocol read or consistent read.
     * @return A stream of transfer batch requests.
     */
    Stream<TransferBatchRequest> createBatchWorkload(List<TransferSegmentRangeSingle> ranges,
                                                     StateTransferType typeOfTransfer) {
        return ranges.stream()
                .filter(range -> range.typeOfTransfer == typeOfTransfer)
                .map(this::getUnknownAddressesInRangeForRange)
                .flatMap(this::rangeToBatchRequestStream);
    }

    /**
     * Transform the transfer segment ranges into the single ones and filter all the non transferred
     * ones.
     *
     * @param beforeTransferRanges Ranges before the transfer, some single and some split.
     * @return Ranges before the transfer, not transferred and single.
     */
    ImmutableList<TransferSegmentRangeSingle> toSingleNotTransferredRanges(
            List<TransferSegmentRange> beforeTransferRanges) {
        return beforeTransferRanges.stream()
                .flatMap(range -> range.toSingle().stream())
                .filter(range -> range.getStatus().getSegmentState() == NOT_TRANSFERRED)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Transform all the transferred ranges back into the transfer segments. This data is used
     * later for cluster reconfiguration.
     *
     * @param transferRanges A list of transfer segment ranges
     * @return A list of transfer segments.
     */
    ImmutableList<TransferSegment> toSegments(List<TransferSegmentRange> transferRanges) {
        return transferRanges.stream().map(TransferSegmentRange::toTransferSegment)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * For all the segment ranges that were not transferred, updated their status.
     *
     * @param newStatus            A new status.
     * @param beforeTransferRanges Ranges before transfer.
     * @return Ranges after transfer.
     */
    ImmutableList<TransferSegmentRange> updateNotTransferredSegmentRangeStatus(TransferSegmentStatus newStatus,
                                                                               List<TransferSegmentRange> beforeTransferRanges) {
        return beforeTransferRanges.stream().map(range -> {
            if (range.getStatus().getSegmentState() == NOT_TRANSFERRED) {
                return range.updateStatus(newStatus);
            }
            return range;
        }).collect(ImmutableList.toImmutableList());
    }

    /**
     * Performs the state transfer for the current non-transferred transfer segments and also
     * updates their state as a result.
     *
     * @param beforeTransferRanges A list of ranges before a transfer.
     * @return A list of segments after a transfer.
     */
    public ImmutableList<TransferSegment> handleTransfer(List<TransferSegmentRange> beforeTransferRanges) {
        // Transform all ranges into single ranges and filter all the not transferred ranges.
        ImmutableList<TransferSegmentRangeSingle> singleNotTransferredRanges =
                toSingleNotTransferredRanges(beforeTransferRanges);

        // If none are NOT_TRANSFERRED, there is nothing to transfer, return the list as is.
        if (singleNotTransferredRanges.isEmpty()) {
            return toSegments(beforeTransferRanges);
        }

        // Split into the protocol and committed workloads.
        Stream<TransferBatchRequest> consistentBatchStream =
                createBatchWorkload(singleNotTransferredRanges, CONSISTENT_READ);

        Stream<TransferBatchRequest> protocolBatchStream =
                createBatchWorkload(singleNotTransferredRanges, PROTOCOL_READ);

        // Execute a parallel transfer first, and then if it succeeds, execute a regular transfer.
        TransferProcessorResult result = parallelTransferProcessor.runStateTransfer(consistentBatchStream)
                .thenCompose(res -> {
                    if (res.getTransferState() == TRANSFER_SUCCEEDED) {
                        return basicTransferProcessor.runStateTransfer(protocolBatchStream);
                    } else {
                        return CompletableFuture.completedFuture(res);
                    }
                }).join();

        log.info("handleTransfer: overall transfer result: {}", result);
        // Update the segment status. If either of the transfers failed the status is failed
        // and if none failed, the status is transferred.
        TransferSegmentStatus newTransferSegmentStatus;

        if (result.getTransferState() == TRANSFER_SUCCEEDED) {
            newTransferSegmentStatus = new TransferSegmentStatus(TRANSFERRED, Optional.empty());
        } else {
            newTransferSegmentStatus = new TransferSegmentStatus(FAILED, result.getCauseOfFailure());
        }

        // Update the status of the not transferred segment ranges.
        ImmutableList<TransferSegmentRange> transferredSegmentRanges =
                updateNotTransferredSegmentRangeStatus(newTransferSegmentStatus,
                        beforeTransferRanges);
        // Transform the segment ranges back into the transfer segments.
        return toSegments(transferredSegmentRanges);
    }


    public interface TransferSegmentRange {
        /**
         * Turn this range into a segment.
         *
         * @return A transfer segment
         */
        TransferSegment toTransferSegment();

        /**
         * Turn this range into a single range.
         *
         * @return A list of one or two single ranges.
         */
        List<TransferSegmentRangeSingle> toSingle();

        /**
         * Update the current transfer segment range with a provided status.
         *
         * @param newStatus A new status
         * @return An updated transfer segment range.
         */
        TransferSegmentRange updateStatus(TransferSegmentStatus newStatus);

        /**
         * Get the status of this transfer segment range.
         *
         * @return A transfer status.
         */
        TransferSegmentStatus getStatus();
    }

    /**
     * A data class that represents a range split of a bounded transfer segment.
     */
    @EqualsAndHashCode
    @Getter
    @ToString
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class TransferSegmentRangeSplit implements TransferSegmentRange {
        @NonNull
        private final Tuple<TransferSegmentRangeSingle, TransferSegmentRangeSingle> splitSegments;

        @Override
        public TransferSegment toTransferSegment() {
            return TransferSegment
                    .builder()
                    .startAddress(splitSegments.first.getStartAddress())
                    .endAddress(splitSegments.second.getEndAddress())
                    .status(splitSegments.first.status.toBuilder().build())
                    .logUnitServers(splitSegments.first.availableServers.orElse(ImmutableList.of()))
                    .build();
        }

        @Override
        public List<TransferSegmentRangeSingle> toSingle() {
            return ImmutableList.of(splitSegments.first, splitSegments.second);
        }

        @Override
        public TransferSegmentRange updateStatus(TransferSegmentStatus newStatus) {
            TransferSegmentRangeSingle first = splitSegments.first.toBuilder()
                    .status(newStatus).build();
            TransferSegmentRangeSingle second = splitSegments.second.toBuilder()
                    .status(newStatus).build();
            return this.toBuilder()
                    .splitSegments(new Tuple<>(first, second)).build();
        }

        @Override
        public TransferSegmentStatus getStatus() {
            return splitSegments.first.status;
        }

        public static class TransferSegmentRangeSplitBuilder {

            public void verify() {
                if (splitSegments.first == null || splitSegments.second == null) {
                    throw new IllegalStateException("Both ranges should be defined.");
                }
                TransferSegmentRangeSingle firstRange = splitSegments.first;
                TransferSegmentRangeSingle secondRange = splitSegments.second;

                if (!firstRange.split) {
                    throw new IllegalStateException("First range should be split.");
                }

                if (!secondRange.split) {
                    throw new IllegalStateException("Second range should be split.");
                }

                if (firstRange.getEndAddress() + 1 != secondRange.getStartAddress()) {
                    throw new IllegalStateException(String.format("End address of a first: %s and " +
                                    "start address of a second: %s " +
                                    "are not consecutive.", firstRange.getEndAddress(),
                            secondRange.getStartAddress()));
                }
                if (!firstRange.getStatus().equals(secondRange.getStatus())) {
                    throw new IllegalStateException(String.format("Status of a first: %s and " +
                                    "status of a second: %s " +
                                    "are not the same.", firstRange.getStatus(),
                            secondRange.getStatus()));
                }

                if (firstRange.getTypeOfTransfer() != CONSISTENT_READ) {
                    throw new IllegalStateException("First range should be of a type " +
                            "consistent read.");
                }

                if (secondRange.getTypeOfTransfer() != PROTOCOL_READ) {
                    throw new IllegalStateException("Second range type should be " +
                            "of a type protocol read.");
                }
            }

            public TransferSegmentRangeSplit build() {
                verify();
                return new TransferSegmentRangeSplit(splitSegments);
            }
        }
    }

    /**
     * A data class that represents a single part of a bounded transfer segment.
     */
    @EqualsAndHashCode
    @Getter
    @ToString
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class TransferSegmentRangeSingle implements TransferSegmentRange {
        private final long startAddress;
        private final long endAddress;
        /**
         * Type of transfer - COMMITTED_READ or PROTOCOL_READ.
         */
        @NonNull
        private final StateTransferType typeOfTransfer;
        /**
         * If the typeOfTransfer field is COMMITTED_READ,
         * it's the list of servers to read committed data from.
         * Otherwise, the field is empty.
         */
        @NonNull
        private final Optional<ImmutableList<String>> availableServers;

        /**
         * The list of the actual non-transferred addresses in the range on this node's log unit server.
         */
        @NonNull
        private final ImmutableList<Long> unknownAddressesInRange;

        /**
         * The transfer status.
         */
        @NonNull
        private final TransferSegmentStatus status;

        /**
         * If a range was split from a segment or not.
         */
        private final boolean split;

        @Override
        public TransferSegment toTransferSegment() {
            return TransferSegment
                    .builder()
                    .startAddress(startAddress)
                    .endAddress(endAddress)
                    .logUnitServers(availableServers.orElse(ImmutableList.of()))
                    .status(status.toBuilder().build())
                    .build();
        }

        @Override
        public List<TransferSegmentRangeSingle> toSingle() {
            return ImmutableList.of(this);
        }

        @Override
        public TransferSegmentRange updateStatus(TransferSegmentStatus newStatus) {
            return this.toBuilder().status(newStatus).build();
        }

        public static class TransferSegmentRangeSingleBuilder {

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

                if (status == null || typeOfTransfer == null || availableServers == null) {
                    throw new IllegalStateException("All non null arguments should be defined.");
                }

                if (unknownAddressesInRange == null) {
                    throw new IllegalStateException("Unknown addresses in range should be defined");
                }
            }

            public TransferSegmentRangeSingle build() {
                verify();
                return new TransferSegmentRangeSingle(startAddress, endAddress, typeOfTransfer,
                        availableServers, unknownAddressesInRange, status, split);
            }
        }
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
         * A list of log unit servers available for this segment.
         */
        private final ImmutableList<String> logUnitServers;

        /**
         * Given an optional committed tail,
         * turn a current transfer segment into a transfer segment range.
         *
         * @param committedTail An optional cluster committed tail.
         * @return A transfer segment range that holds the data necessary to execute the transfer.
         */
        public TransferSegmentRange toTransferSegmentRange(Optional<Long> committedTail) {
            return committedTail.map(tail -> {
                if (tail < startAddress) {
                    // If the tail is below the start address, this segment is a single range
                    // that is to be transferred via replication protocol.
                    return TransferSegmentRangeSingle
                            .builder()
                            .startAddress(startAddress)
                            .endAddress(endAddress)
                            .typeOfTransfer(PROTOCOL_READ)
                            .availableServers(Optional.empty())
                            .split(false)
                            .status(getStatus().toBuilder().build())
                            .unknownAddressesInRange(ImmutableList.of())
                            .build();
                } else if (tail >= endAddress) {
                    // If the tail is greater or equal to the end address, this segment is a single
                    // range that is to be transferred via consistent read protocol.
                    return TransferSegmentRangeSingle
                            .builder()
                            .startAddress(startAddress)
                            .endAddress(endAddress)
                            .typeOfTransfer(CONSISTENT_READ)
                            .availableServers(Optional.of(ImmutableList.copyOf(getLogUnitServers())))
                            .split(false)
                            .status(getStatus().toBuilder().build())
                            .unknownAddressesInRange(ImmutableList.of())
                            .build();
                } else {
                    // If the tail is in the middle of a transfer segment, this segment is a split
                    // range, the first half of it will be transferred via consistent read, and
                    // the second half - via replication protocol.
                    TransferSegmentRangeSingle first = TransferSegmentRangeSingle
                            .builder()
                            .startAddress(startAddress)
                            .endAddress(tail)
                            .typeOfTransfer(CONSISTENT_READ)
                            .availableServers(Optional.of(ImmutableList.copyOf(getLogUnitServers())))
                            .split(true)
                            .status(getStatus().toBuilder().build())
                            .unknownAddressesInRange(ImmutableList.of())
                            .build();

                    TransferSegmentRangeSingle second = TransferSegmentRangeSingle
                            .builder()
                            .startAddress(tail + 1)
                            .endAddress(endAddress)
                            .typeOfTransfer(PROTOCOL_READ)
                            .availableServers(Optional.empty())
                            .split(true)
                            .status(getStatus().toBuilder().build())
                            .unknownAddressesInRange(ImmutableList.of())
                            .build();

                    return TransferSegmentRangeSplit.builder()
                            .splitSegments(new Tuple<>(first, second)).build();
                }
                // If the committed tail is not present (we failed to retrieve it earlier),
                // this segment is a single range and is to be transferred via replication protocol.
            }).orElse(TransferSegmentRangeSingle
                    .builder()
                    .startAddress(startAddress)
                    .endAddress(endAddress)
                    .typeOfTransfer(PROTOCOL_READ)
                    .availableServers(Optional.empty())
                    .split(false)
                    .status(getStatus().toBuilder().build())
                    .unknownAddressesInRange(ImmutableList.of())
                    .build());
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

                if (logUnitServers == null) {
                    throw new IllegalStateException("Log unit servers should be present.");
                }
            }

            public TransferSegment build() {
                verify();
                return new TransferSegment(startAddress, endAddress, status, logUnitServers);
            }
        }
    }

    /**
     * A data class that represents a status of a segment to be transferred.
     */
    @Getter
    @ToString
    @EqualsAndHashCode
    @Builder(toBuilder = true)
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
         * An optional cause of failure for this segment.
         */
        @Default
        @Exclude
        private final Optional<TransferSegmentException> causeOfFailure = Optional.empty();
    }
}
