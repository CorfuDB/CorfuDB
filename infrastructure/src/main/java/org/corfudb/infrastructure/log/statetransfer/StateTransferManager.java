package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.segment.StateTransferType;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegment;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentRange;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentRangeSingle;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus;
import org.corfudb.infrastructure.log.statetransfer.transferprocessor.BasicTransferProcessor;
import org.corfudb.infrastructure.log.statetransfer.transferprocessor.ParallelTransferProcessor;
import org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessorResult;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.util.CFUtils;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.corfudb.infrastructure.log.statetransfer.segment.StateTransferType.CONSISTENT_READ;
import static org.corfudb.infrastructure.log.statetransfer.segment.StateTransferType.PROTOCOL_READ;
import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_FAILED;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_SUCCEEDED;

/**
 * A class responsible for managing a state transfer on the current node.
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
        Set<Long> knownAddresses = CFUtils.getUninterruptibly(logUnitClient
                .requestKnownAddresses(rangeStart, rangeEnd))
                .getKnownAddresses();

        return LongStream.range(rangeStart, rangeEnd + 1L)
                .filter(address -> !knownAddresses.contains(address))
                .boxed()
                .collect(ImmutableList.toImmutableList());
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
        ImmutableList<Long> unknownAddressesInRange = range.getUnknownAddressesInRange();
        Optional<ImmutableList<String>> availableServers = range.getAvailableServers();
        return Lists.partition(unknownAddressesInRange, batchSize).stream()
                .map(partition -> TransferBatchRequest
                        .builder()
                        .addresses(partition)
                        .destinationNodes(availableServers)
                        .build());
    }

    /**
     * Create a batch workload for a single segment.
     *
     * @param range A single range
     * @return A stream of transfer batch requests from  range.
     */
    private Stream<TransferBatchRequest> createBatchWorkloadForSegment(
            TransferSegmentRangeSingle range) {
        TransferSegmentRangeSingle rangeWithUnknownAddresses =
                getUnknownAddressesInRangeForRange(range);
        return rangeToBatchRequestStream(rangeWithUnknownAddresses);
    }

    /**
     * Go over all the single transfer segment ranges, filter them by the provided type,
     * get all the unknown addresses in range, and then turn the unknown addresses in range into
     * the transfer batch request stream - ready to be consumed by the appropriate
     * state transfer processor.
     *
     * @param ranges         A list of single transfer segment ranges.
     * @param typeOfTransfer A provided type of transfer - protocol read or consistent read.
     * @return A lists of streams of transfer batch requests.
     */
    List<Stream<TransferBatchRequest>> createBatchWorkload(List<TransferSegmentRangeSingle> ranges,
                                                           StateTransferType typeOfTransfer) {
        return ranges.stream()
                .filter(range -> range.getTypeOfTransfer() == typeOfTransfer)
                .map(this::createBatchWorkloadForSegment)
                .collect(ImmutableList.toImmutableList());
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
     * Run the committed state transfer.
     *
     * @param consistentBatchStreamList     A list of batch streams for each segment.
     * @param parallelismFactorForEachRange A list of parallelism factors for each stream.
     * @return A future of transfer processor result.
     */
    private CompletableFuture<TransferProcessorResult> runCommittedStateTransfer(
            List<Stream<TransferBatchRequest>> consistentBatchStreamList,
            List<Integer> parallelismFactorForEachRange) {
        if (consistentBatchStreamList.size() != parallelismFactorForEachRange.size()) {
            throw new IllegalArgumentException("The lists of streams and par factors" +
                    " should be equal.");
        }
        for (int i = 0; i < parallelismFactorForEachRange.size(); i++) {
            int parFactor = parallelismFactorForEachRange.get(i);
            Stream<TransferBatchRequest> transferBatchRequestStream =
                    consistentBatchStreamList.get(i);
            TransferProcessorResult parallelTransferResult = parallelTransferProcessor
                    .runStateTransfer(transferBatchRequestStream, parFactor)
                    .join();
            if (parallelTransferResult.getTransferState() == TRANSFER_FAILED) {
                return CompletableFuture.completedFuture(parallelTransferResult);
            }
        }
        return CompletableFuture.completedFuture(TransferProcessorResult.builder().build());
    }

    /**
     * Run the protocol state transfer.
     *
     * @param protocolBatchStream Stream of the batch requests for all the protocol segments.
     * @return A future of transfer processor result.
     */
    private CompletableFuture<TransferProcessorResult> runProtocolStateTransfer(
            Stream<TransferBatchRequest> protocolBatchStream) {
        return basicTransferProcessor.runStateTransfer(protocolBatchStream);
    }

    /**
     * For all the segment ranges that were not transferred, updated their status.
     *
     * @param newStatus            A new status.
     * @param beforeTransferRanges Ranges before transfer.
     * @return Ranges after transfer.
     */
    private ImmutableList<TransferSegmentRange> updateNotTransferredSegmentRangeStatus(
            TransferSegmentStatus newStatus, List<TransferSegmentRange> beforeTransferRanges) {
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
        List<Stream<TransferBatchRequest>> consistentBatchStreamList =
                createBatchWorkload(singleNotTransferredRanges, CONSISTENT_READ);

        Stream<TransferBatchRequest> protocolBatchStream =
                createBatchWorkload(singleNotTransferredRanges, PROTOCOL_READ)
                        .stream().flatMap(Function.identity());

        // For the consistent transfer get the parallelism factors for each segment,
        // which is equal to num nodes for each segment range.
        List<Integer> parallelismFactorForEachRange = singleNotTransferredRanges
                .stream()
                .filter(range -> range.getTypeOfTransfer() == CONSISTENT_READ)
                .map(range -> {
                    if (range.getAvailableServers().isPresent()) {
                        ImmutableList<String> strings = range.getAvailableServers().get();
                        if (strings.isEmpty()) {
                            throw new IllegalStateException("Server list is empty.");
                        }
                        return strings.size();
                    } else {
                        throw new IllegalStateException("No available servers " +
                                "for consistent transfer.");
                    }

                })
                .collect(Collectors.toList());

        // Execute a parallel transfer first, and then if it succeeds, execute a regular transfer.
        TransferProcessorResult result = runCommittedStateTransfer(consistentBatchStreamList,
                parallelismFactorForEachRange)
                .thenCompose(res -> {
                    if (res.getTransferState() == TRANSFER_SUCCEEDED) {
                        return runProtocolStateTransfer(protocolBatchStream);
                    } else {
                        return CompletableFuture.completedFuture(res);
                    }
                }).join();

        log.info("handleTransfer: overall transfer result: {}", result);
        // Update the segment status. If either of the transfers failed the status is failed
        // and if none failed, the status is transferred.
        TransferSegmentStatus newTransferSegmentStatus;

        if (result.getTransferState() == TRANSFER_SUCCEEDED) {
            newTransferSegmentStatus = TransferSegmentStatus.builder().segmentState(TRANSFERRED)
                    .causeOfFailure(Optional.empty()).build();
        } else {
            newTransferSegmentStatus = TransferSegmentStatus.builder().segmentState(FAILED)
                    .causeOfFailure(result.getCauseOfFailure()).build();
        }

        // Update the status of the not transferred segment ranges.
        ImmutableList<TransferSegmentRange> transferredSegmentRanges =
                updateNotTransferredSegmentRangeStatus(newTransferSegmentStatus,
                        beforeTransferRanges);
        // Transform the segment ranges back into the transfer segments.
        return toSegments(transferredSegmentRanges);
    }
}
