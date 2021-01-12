package org.corfudb.infrastructure.log.statetransfer.segment;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.common.util.Tuple;

import java.util.List;

import static org.corfudb.infrastructure.log.statetransfer.segment.StateTransferType.CONSISTENT_READ;
import static org.corfudb.infrastructure.log.statetransfer.segment.StateTransferType.PROTOCOL_READ;

/**
 * A data class that represents a range split of a bounded transfer segment.
 */
@EqualsAndHashCode
@Getter
@ToString
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TransferSegmentRangeSplit implements TransferSegmentRange {
    @NonNull
    private final Tuple<TransferSegmentRangeSingle, TransferSegmentRangeSingle> splitSegments;


    @Override
    public TransferSegment toTransferSegment() {
        return TransferSegment
                .builder()
                .startAddress(splitSegments.first.getStartAddress())
                .endAddress(splitSegments.second.getEndAddress())
                .status(splitSegments.first.getStatus().toBuilder().build())
                .logUnitServers(splitSegments.first.getAvailableServers().orElse(ImmutableList.of()))
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
        return splitSegments.first.getStatus();
    }

    public static class TransferSegmentRangeSplitBuilder {

        public void verify() {
            if (splitSegments.first == null || splitSegments.second == null) {
                throw new IllegalStateException("Both ranges should be defined.");
            }
            TransferSegmentRangeSingle firstRange = splitSegments.first;
            TransferSegmentRangeSingle secondRange = splitSegments.second;

            if (!firstRange.isSplit()) {
                throw new IllegalStateException("First range should be split.");
            }

            if (!secondRange.isSplit()) {
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
