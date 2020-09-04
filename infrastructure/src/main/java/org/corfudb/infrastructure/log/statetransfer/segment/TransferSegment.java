package org.corfudb.infrastructure.log.statetransfer.segment;

import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.common.util.Tuple;

import java.util.Optional;

import static org.corfudb.infrastructure.log.statetransfer.segment.StateTransferType.CONSISTENT_READ;
import static org.corfudb.infrastructure.log.statetransfer.segment.StateTransferType.PROTOCOL_READ;

/**
 * A data class that represents a non-empty and bounded segment to be transferred.
 */
@EqualsAndHashCode
@Getter
@ToString
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TransferSegment {
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
