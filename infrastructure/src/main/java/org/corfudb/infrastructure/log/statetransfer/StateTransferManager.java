package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferFailure;
import org.corfudb.runtime.view.Address;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.*;
import static org.corfudb.runtime.view.Address.*;

@Slf4j
@AllArgsConstructor
/**
 * This class is responsible for managing a state transfer on the current node.
 */
public class StateTransferManager {

    public enum SegmentState {
        NOT_TRANSFERRED,
        TRANSFERRED,
        RESTORED,
        FAILED
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    @ToString
    public static class CurrentTransferSegment {
        private final long startAddress;
        private final long endAddress;
    }

    @Getter
    @ToString
    @EqualsAndHashCode
    public static class CurrentTransferSegmentStatus {
        private SegmentState segmentStateTransferState;
        private long lastTransferredAddress;
        private StateTransferFailure causeOfFailure = null;

        public CurrentTransferSegmentStatus(SegmentState segmentStateTransferState,
                                            long lastTransferredAddress){
            this.segmentStateTransferState = segmentStateTransferState;
            this.lastTransferredAddress = lastTransferredAddress;
        }

        public CurrentTransferSegmentStatus(SegmentState segmentStateTransferState,
                                            long lastTransferredAddress,
                                            StateTransferFailure causeOfFailure){
            this.segmentStateTransferState = segmentStateTransferState;
            this.lastTransferredAddress = lastTransferredAddress;
            this.causeOfFailure = causeOfFailure;
        }
    }

    @Getter
    @NonNull
    private final StreamLog streamLog;

    @Getter
    @NonNull
    private final StateTransferWriter stateTransferWriter;

    @Getter
    @NonNull
    private final int batchSize;

    List<Long> getUnknownAddressesInRange(long rangeStart, long rangeEnd) {
        Set<Long> knownAddresses = streamLog
                .getKnownAddressesInRange(rangeStart, rangeEnd);

        return LongStream.range(rangeStart, rangeEnd + 1)
                .filter(address -> !knownAddresses.contains(address))
                .boxed()
                .collect(Collectors.toList());
    }

    public ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
    handleTransfer(Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> statusMap) {
        return ImmutableMap.copyOf(statusMap.entrySet().stream().map(entry -> {
            CurrentTransferSegment segment = entry.getKey();
            CompletableFuture<CurrentTransferSegmentStatus> status = entry.getValue();

            if (status.isCompletedExceptionally()) {
                // If a future failed exceptionally, mark as failed.
                CompletableFuture<CurrentTransferSegmentStatus> failedStatus =
                        status.handle((value, exception) ->
                        new CurrentTransferSegmentStatus(FAILED,
                        NON_ADDRESS, new StateTransferFailure(exception)));

                return new SimpleEntry<>(segment, failedStatus);
            } else if (!status.isDone()) {
                // It's still in progress.
                return new SimpleEntry<>(segment, status);
            } else {
                //  - NOT_TRANSFERRED
                CurrentTransferSegmentStatus statusJoin = status.join();
                SimpleEntry<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> result;
                if (statusJoin.getSegmentStateTransferState().equals(NOT_TRANSFERRED)) {
                    List<Long> unknownAddressesInRange =
                            getUnknownAddressesInRange(segment.getStartAddress(), segment.getEndAddress());
                    if (unknownAddressesInRange.isEmpty()) {
                        // no addresses to transfer - all done
                        CurrentTransferSegmentStatus currentTransferSegmentStatus =
                                new CurrentTransferSegmentStatus(TRANSFERRED, segment.getEndAddress());
                        result = new SimpleEntry<>(segment, CompletableFuture.completedFuture(currentTransferSegmentStatus));
                    } else { // transfer whatever is not transferred

                        Long lastAddressToTransfer =
                                unknownAddressesInRange.get(unknownAddressesInRange.size() - 1);
                        CompletableFuture<CurrentTransferSegmentStatus> segmentStatusFuture =
                                stateTransferWriter
                                        .stateTransfer(unknownAddressesInRange, batchSize)
                                        .thenApply(lastTransferredAddressResult -> {
                                            if (lastTransferredAddressResult.isValue() &&
                                                    lastTransferredAddressResult.get().equals(lastAddressToTransfer)) {
                                                long lastTransferredAddress = lastTransferredAddressResult.get();
                                                log.info("State transfer segment success, transferred up to: {}.",
                                                        lastTransferredAddress);
                                                return new CurrentTransferSegmentStatus(TRANSFERRED, lastTransferredAddress);
                                            }
                                            else if(lastTransferredAddressResult.isValue() &&
                                                    !lastTransferredAddressResult.get().equals(lastAddressToTransfer)){
                                                log.error("Incomplete transfer failure occurred, " +
                                                        "expected last address to be: {}, but it's: {}",
                                                        lastAddressToTransfer, lastTransferredAddressResult.get());
                                                return new CurrentTransferSegmentStatus(FAILED,
                                                        lastTransferredAddressResult.get(),
                                                        new StateTransferFailure("Incomplete transfer failure."));
                                            }

                                            else {
                                                log.error("State transfer failure occurred: ",
                                                        lastTransferredAddressResult.getError().getCause());
                                                return new CurrentTransferSegmentStatus(
                                                        FAILED,
                                                        NON_ADDRESS,
                                                        (StateTransferFailure) lastTransferredAddressResult.getError());
                                            }
                                        });
                        result = new SimpleEntry<>(segment, segmentStatusFuture);
                    }
                }
                //  - RESTORED
                else {

                    result = new SimpleEntry<>(segment, CompletableFuture.completedFuture(statusJoin));
                }
                return result;

            }
        }).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)));

    }
}