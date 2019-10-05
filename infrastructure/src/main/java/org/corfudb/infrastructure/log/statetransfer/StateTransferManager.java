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
import org.corfudb.runtime.view.Address;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
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

    @AllArgsConstructor
    @Getter
    @ToString
    @EqualsAndHashCode
    public static class CurrentTransferSegmentStatus {
        private SegmentState segmentStateTransferState;
        private long lastTransferredAddress;
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
                return new SimpleEntry<>(segment, CompletableFuture
                        .completedFuture(new CurrentTransferSegmentStatus(FAILED,
                                NON_ADDRESS)));
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
                                                return new CurrentTransferSegmentStatus(TRANSFERRED, lastTransferredAddress);
                                            } else {
                                                return new CurrentTransferSegmentStatus(FAILED, NON_ADDRESS);
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