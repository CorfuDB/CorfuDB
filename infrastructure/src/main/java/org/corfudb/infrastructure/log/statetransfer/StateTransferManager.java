package org.corfudb.infrastructure.log.statetransfer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.StreamLog;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.*;

@Slf4j
@Builder
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
    public static class CurrentTransferSegment implements Comparable<CurrentTransferSegment> {
        private final long startAddress;
        private final long endAddress;

        @Override
        public int compareTo(CurrentTransferSegment other) {
            return (int) (this.startAddress - other.endAddress);
        }
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
    private StreamLog streamLog;

    @Getter
    @NonNull
    private StateTransferWriter stateTransferWriter;

    private List<Long> getUnknownAddressesInRange(long rangeStart, long rangeEnd) {

        Set<Long> knownAddresses = streamLog
                .getKnownAddressesInRange(rangeStart, rangeEnd);

        return LongStream.range(rangeStart, rangeEnd + 1)
                .filter(address -> !knownAddresses.contains(address))
                .boxed()
                .collect(Collectors.toList());
    }


    public List<SimpleEntry<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>>
    handleTransfer(Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> statusMap) {
        return statusMap.entrySet().stream().map(entry -> {
            CurrentTransferSegment segment = entry.getKey();
            CompletableFuture<CurrentTransferSegmentStatus> status = entry.getValue();
            if(status.isCompletedExceptionally()){
                // If a future failed exceptionally, mark as failed.
                return new SimpleEntry<>(segment, CompletableFuture
                        .completedFuture(new CurrentTransferSegmentStatus(FAILED,
                                -1L)));
            }
            else if(!status.isDone()){
                // It's still in progress.
                return new SimpleEntry<>(segment, status);
            }
            else{ // Future is completed -> NOT_TRANSFERRED or RESTORED
                CurrentTransferSegmentStatus statusJoin = status.join();
                SimpleEntry<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> result;
                if (statusJoin.getSegmentStateTransferState().equals(NOT_TRANSFERRED)) {
                    List<Long> unknownAddressesInRange =
                            getUnknownAddressesInRange(segment.getStartAddress(), segment.getEndAddress());

                    if(unknownAddressesInRange.isEmpty()){
                        // no addresses to transfer - all done
                        CurrentTransferSegmentStatus currentTransferSegmentStatus =
                                new CurrentTransferSegmentStatus(TRANSFERRED, segment.getEndAddress());
                        result = new SimpleEntry<>(segment, CompletableFuture.completedFuture(currentTransferSegmentStatus));
                    }
                    else{
                        CompletableFuture<CurrentTransferSegmentStatus> segmentStatusFuture =
                                stateTransferWriter
                                        .stateTransfer(unknownAddressesInRange)
                                        .thenApply(r -> {
                                            if (r.isValue() && r.get().equals(segment.getEndAddress())) {
                                                return new CurrentTransferSegmentStatus(TRANSFERRED, r.get());
                                            } else {
                                                return new CurrentTransferSegmentStatus(FAILED, segment.startAddress);
                                            }
                                        });
                        result = new SimpleEntry<>(segment, segmentStatusFuture);
                    }
                }
                else {

                    result = new SimpleEntry<>(segment, CompletableFuture.completedFuture(statusJoin));
                }
                return result;

            }


        }).collect(Collectors.toList());

    }
}