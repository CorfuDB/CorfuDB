package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFactory;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFactory.BatchProcessorType;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessorData;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessor;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessorData;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.StreamProcessFailure;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy.StaticPolicyData;
import org.corfudb.util.CFUtils;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessor.SlidingWindow;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFactory.BatchProcessorType.COMMITTED;
import static org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFactory.BatchProcessorType.PROTOCOL;
import static org.corfudb.runtime.view.Address.NON_ADDRESS;

/**
 * A class responsible for managing a state transfer on the current node.
 * It executes the state transfer for each non-transferred segment asynchronously
 * and keeps track of every transfer segment state.
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
    public static class CurrentTransferSegment {
        /**
         * Start address of a segment.
         */
        private final long startAddress;
        /**
         * End address of a segment.
         */
        private final long endAddress;
        /**
         * A future that hold the status of a transfer of a segment.
         */
        private final CompletableFuture<CurrentTransferSegmentStatus> status;
        /**
         * An optional data that could be utilized to transfer this segment more efficiently.
         */
        private final Optional<CommittedTransferData> committedTransferData;

        public CurrentTransferSegment(long startAddress, long endAddress,
                                      CompletableFuture<CurrentTransferSegmentStatus> status) {
            this.startAddress = startAddress;
            this.endAddress = endAddress;
            this.status = status;
            this.committedTransferData = Optional.empty();
        }

        public CurrentTransferSegment(long startAddress, long endAddress,
                                      CompletableFuture<CurrentTransferSegmentStatus> status,
                                      Optional<CommittedTransferData> committedTransferData) {
            this.startAddress = startAddress;
            this.endAddress = endAddress;
            this.status = status;
            this.committedTransferData = committedTransferData;
        }
    }

    /**
     * A data class that represent a status of a segment to be transferred.
     */
    @Getter
    @ToString
    @EqualsAndHashCode
    public static class CurrentTransferSegmentStatus {
        /**
         * A state of a segment.
         */
        private final SegmentState segmentState;
        /**
         * Total number of records transferred for this segment.
         */
        private final long totalTransferred;
        /**
         * An optional cause of failure for this segment.
         */
        private final Optional<StreamProcessFailure> causeOfFailure;

        public CurrentTransferSegmentStatus(SegmentState segmentState,
                                            long totalTransferred) {
            this.segmentState = segmentState;
            this.totalTransferred = totalTransferred;
            this.causeOfFailure = Optional.empty();
        }

        public CurrentTransferSegmentStatus(SegmentState segmentState,
                                            long totalTransferred,
                                            Optional<StreamProcessFailure> causeOfFailure) {
            this.segmentState = segmentState;
            this.totalTransferred = totalTransferred;
            this.causeOfFailure = causeOfFailure;
        }
    }

    /**
     * A data class that hold the information needed to transfer the committed records.
     */
    @AllArgsConstructor
    @Getter
    public static class CommittedTransferData {
        /**
         * An address of the last committed record.
         */
        private final long committedOffset;
        /**
         * A list of servers that a state transfer processor can use
         * to transfer the committed records directly, bypassing the consistency protocol.
         */
        private final List<String> sourceServers;
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

        return ImmutableList.copyOf(LongStream.range(rangeStart, rangeEnd + 1)
                .filter(address -> !knownAddresses.contains(address))
                .boxed()
                .collect(Collectors.toList()));
    }

    /**
     * Perform the state transfer for the current transfer segments and also
     * update their state as a result.
     *
     * @param currentTransferSegments A list of the segment currently present in the system.
     * @param batchProcessorData      A piece of data needed to select
     *                                an appropriate batch processor instance.
     * @return A list with the updated transfer segments.
     */
    public ImmutableList<CurrentTransferSegment> handleTransfer(List<CurrentTransferSegment> currentTransferSegments,
                                                                StateTransferBatchProcessorData
                                                                        batchProcessorData) {
        List<CurrentTransferSegment> finalList = currentTransferSegments.stream().map(segment ->
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

                                        return CompletableFuture.completedFuture(
                                                new CurrentTransferSegmentStatus(TRANSFERRED,
                                                        segment.getStartAddress() -
                                                                segment.getEndAddress() + 1L)
                                        );
                                    } else {
                                        // Get total number of addresses needed to transfer.
                                        long numAddressesToTransfer = unknownAddressesInRange.size();
                                        // Get the data about the committed records within this segment.
                                        Optional<CommittedTransferData> committedTransferData =
                                                segment.getCommittedTransferData();
                                        // Create a state transfer config.
                                        StateTransferConfig config = StateTransferConfig.builder()
                                                .unknownAddresses(unknownAddressesInRange)
                                                .committedTransferData(committedTransferData)
                                                .batchSize(batchSize)
                                                .batchProcessorData(batchProcessorData).build();
                                        // State transfer and then update
                                        // the status based on the result.
                                        return stateTransfer(config).thenApply(result ->
                                                createStatusBasedOnTransferResult(result,
                                                        numAddressesToTransfer));

                                    }
                                    // If a segment contains some other status -> return.
                                } else {
                                    return CompletableFuture.completedFuture(status);
                                }
                            });
                    return new CurrentTransferSegment(segment.getStartAddress(),
                            segment.getEndAddress(), newStatus);
                }


        ).collect(Collectors.toList());

        return ImmutableList.copyOf(finalList);

    }

    /**
     * Perform a state transfer for this segment.
     * @param stateTransferConfig A state transfer config object.
     * @return A result, containing a total number of records transferred or a failure.
     */
    CompletableFuture<Result<Long, StreamProcessFailure>> stateTransfer(
            StateTransferConfig stateTransferConfig) {
        return configureStateTransfer(stateTransferConfig).get();
    }

    /**
     * Based on a result of a state transfer, update a segment status.
     * @param result A result, containing a total number of records transferred or a failure.
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
            return new CurrentTransferSegmentStatus(FAILED, 0L, Optional.of(checkedResult.getError()));
        } else {
            return new CurrentTransferSegmentStatus(TRANSFERRED, checkedResult.get(), Optional.empty());
        }
    }

    /**
     * Based on a state transfer config, decide what part of a segment
     * is transferred via a replication protocol, and which part is transferred
     * via a committed batch processor.
     * @param stateTransferConfig A config needed to execute a state transfer on the current segment.
     * @return A function that executes a state transfer for this segment.
     */
    Supplier<CompletableFuture<Result<Long, StreamProcessFailure>>>
    configureStateTransfer(StateTransferConfig stateTransferConfig) {

        Optional<CommittedTransferData> possibleCommittedTransferData =
                stateTransferConfig.getCommittedTransferData();

        List<Long> unknownAddresses =
                stateTransferConfig.getUnknownAddresses();

        StateTransferBatchProcessorData batchProcessorData =
                stateTransferConfig.getBatchProcessorData();

        int batchSize = stateTransferConfig.getBatchSize();

        PolicyStreamProcessorData policyStreamProcessorData =
                stateTransferConfig.getPolicyStreamProcessorData();

        // There exists a range of addresses that can be transferred with a committed batch processor.
        if (possibleCommittedTransferData.isPresent()) {
            CommittedTransferData committedTransferData = possibleCommittedTransferData.get();
            long committedOffset = committedTransferData.getCommittedOffset();
            List<String> sourceServers = committedTransferData.getSourceServers();
            int windowSize = sourceServers.size() * batchSize;

            int indexOfTheLastCommittedAddress =
                    Collections.binarySearch(unknownAddresses, committedOffset);
            int indexOfTheFirstNonCommittedAddress = indexOfTheLastCommittedAddress + 1;
            int indexOfTheLastAddress = unknownAddresses.size() - 1;

            List<Long> committedAddresses =
                    IntStream.range(0, indexOfTheLastCommittedAddress + 1)
                            .boxed().map(unknownAddresses::get).collect(Collectors.toList());

            StaticPolicyData committedStaticPolicyData =
                    new StaticPolicyData(committedAddresses, Optional.of(sourceServers), batchSize);

            PolicyStreamProcessor committedStreamProcessor = createStreamProcessor(batchProcessorData,
                    policyStreamProcessorData, COMMITTED, windowSize);

            // Part of records is transferred via a protocol and part via a committed processor.
            if (indexOfTheFirstNonCommittedAddress <= indexOfTheLastAddress) {
                List<Long> nonCommittedAddresses =
                        IntStream.range(indexOfTheFirstNonCommittedAddress, indexOfTheLastAddress)
                                .boxed().map(unknownAddresses::get).collect(Collectors.toList());

                StaticPolicyData protocolStaticPolicyData =
                        new StaticPolicyData(nonCommittedAddresses, Optional.empty(), batchSize);

                PolicyStreamProcessor protocolStreamProcessor = createStreamProcessor(batchProcessorData,
                        policyStreamProcessorData, PROTOCOL, windowSize);

                return () -> coalesceResults(ImmutableList.of(protocolStreamProcessor
                                .processStream(protocolStaticPolicyData),
                        committedStreamProcessor.processStream(committedStaticPolicyData)));
            } else {
                return () -> committedStreamProcessor.processStream(committedStaticPolicyData);
            }
            // Everything is transferred via a protocol.
        } else {
            PolicyStreamProcessor protocolStreamProcessor = createStreamProcessor(batchProcessorData,
                    policyStreamProcessorData, PROTOCOL, batchSize);

            StaticPolicyData protocolStaticPolicyData =
                    new StaticPolicyData(unknownAddresses, Optional.empty(), batchSize);


            return () -> protocolStreamProcessor
                    .processStream(protocolStaticPolicyData);
        }
    }

    /**
     * Create a stream processor instance.
     * @param batchProcessorData A data needed to initiate a batch processor.
     * @param policyStreamProcessorData Policies that this stream processor
     *                                  will use to aid with a state transfer.
     * @param type A type of a batch processor to create.
     * @param windowSize A size of a {@link SlidingWindow}.
     * @return An instance of a stream processor with predefined policies.
     */
    PolicyStreamProcessor createStreamProcessor(StateTransferBatchProcessorData batchProcessorData,
                                                PolicyStreamProcessorData policyStreamProcessorData,
                                                BatchProcessorType type,
                                                int windowSize) {
        StateTransferBatchProcessor protocolBatchProcessor =
                BatchProcessorFactory.createBatchProcessor(batchProcessorData, type);
        return PolicyStreamProcessor
                .builder()
                .windowSize(windowSize)
                .dynamicProtocolWindowSize(windowSize)
                .policyData(policyStreamProcessorData)
                .batchProcessor(protocolBatchProcessor).build();
    }

    /**
     * If the parts of a transfer were transferred by using different mechanics,
     * combine the results.
     * @param allResults All the results of the state transfer for the segment.
     * @return A result containing a total number of addresses transferred
     * or an exception of either one of them.
     */
    CompletableFuture<Result<Long, StreamProcessFailure>> coalesceResults
            (List<CompletableFuture<Result<Long, StreamProcessFailure>>> allResults) {
        CompletableFuture<List<Result<Long, StreamProcessFailure>>> futureOfListResults =
                CFUtils.sequence(allResults);

        CompletableFuture<Optional<Result<Long, StreamProcessFailure>>> possibleSingleResult = futureOfListResults
                .thenApply(multipleResults ->
                        multipleResults
                                .stream()
                                .reduce(this::mergeTransferResults));

        return possibleSingleResult.thenApply(result -> result.orElseGet(() ->
                new Result<>(NON_ADDRESS,
                        new StreamProcessFailure(
                                new IllegalStateException("Coalesced transfer batch result is empty.")))));

    }

    /**
     * Add the total number of addresses transferred from both the results
     * or propagate their exceptions.
     * @param firstResult A result of a first transfer.
     * @param secondResult A result of a second transfer.
     * @return A combined result.
     */
    Result<Long, StreamProcessFailure> mergeTransferResults(Result<Long, StreamProcessFailure> firstResult,
                                                            Result<Long, StreamProcessFailure> secondResult) {
        return firstResult.flatMap(firstTotal ->
                secondResult.map(secondTotal ->
                        firstTotal + secondTotal));
    }
}