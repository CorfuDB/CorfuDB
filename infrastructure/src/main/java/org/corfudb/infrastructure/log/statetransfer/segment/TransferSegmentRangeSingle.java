package org.corfudb.infrastructure.log.statetransfer.segment;

import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.List;
import java.util.Optional;

/**
 * A data class that represents a single part of a bounded transfer segment.
 */
@EqualsAndHashCode
@Getter
@ToString
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TransferSegmentRangeSingle implements TransferSegmentRange {
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
