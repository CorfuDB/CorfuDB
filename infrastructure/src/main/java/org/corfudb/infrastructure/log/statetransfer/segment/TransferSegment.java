package org.corfudb.infrastructure.log.statetransfer.segment;

import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.common.util.Tuple;
import org.corfudb.runtime.view.Address;

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
    @NonNull
    private final TransferSegmentStatus status;

    /**
     * A list of log unit servers available for this segment.
     */
    @NonNull
    private final ImmutableList<String> logUnitServers;

    /**
     * Given an optional committed tail,
     * turn a current transfer segment into a transfer segment range.
     *
     * @param committedTail An optional cluster committed tail.
     * @return A transfer segment range that holds the data necessary to execute the transfer.
     */
    public TransferSegmentRange toTransferSegmentRange(Optional<Long> committedTail) {
        return committedTail
                .map(this::getTransferSegmentRange)
                .orElse(getProtocolTransferSegment());
    }

    private TransferSegmentRange getTransferSegmentRange(Long tail) {
        if (tail < startAddress) {
            return getProtocolTransferSegment();
        } else if (tail >= endAddress) {
            return getConsistentReadSegment();
        } else {
            return getSplitRangeSegment(tail);
        }
        // If the committed tail is not present (we failed to retrieve it earlier),
        // this segment is a single range and is to be transferred via replication protocol.
    }

    /**
     * If the tail is below the start address, this segment is a single range
     * that is to be transferred via replication protocol.
     * @return protocol read segment
     */
    private TransferSegmentRangeSingle getProtocolTransferSegment() {
        return TransferSegmentRangeSingle
                .builder()
                .startAddress(startAddress)
                .endAddress(endAddress)
                .typeOfTransfer(PROTOCOL_READ)
                .availableServers(Optional.empty())
                .split(false)
                .status(status)
                .unknownAddressesInRange(ImmutableList.of())
                .build();
    }

    /**
     * If the tail is in the middle of a transfer segment, this segment is a split
     * range, the first half of it will be transferred via consistent read, and
     * the second half - via replication protocol.
     * @param tail tail
     * @return split range segment
     */
    private TransferSegmentRangeSplit getSplitRangeSegment(Long tail) {
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
                .status(status)
                .unknownAddressesInRange(ImmutableList.of())
                .build();

        return TransferSegmentRangeSplit.builder()
                .splitSegments(new Tuple<>(first, second))
                .build();
    }

    /**
     * If the tail is greater or equal to the end address, this segment is a single
     * range that is to be transferred via consistent read protocol.
     * @return consistent read segment
     */
    private TransferSegmentRangeSingle getConsistentReadSegment() {
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
    }

    public static class TransferSegmentBuilder {

        public void verify() {
            long minAddress = Address.getMinAddress();
            if (startAddress < minAddress || endAddress < minAddress) {
                throw new IllegalStateException(
                        String.format("Start: %s or end: %s " +
                                "can not be negative.", startAddress, endAddress));
            }
            if (startAddress > endAddress) {
                String errStrMsg = "Start: %s can not be greater than end: %s.";
                String errStr = String.format(errStrMsg, startAddress, endAddress);
                throw new IllegalStateException(errStr);
            }
        }

        public TransferSegment build() {
            verify();
            return new TransferSegment(startAddress, endAddress, status, logUnitServers);
        }
    }
}
