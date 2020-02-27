package org.corfudb.infrastructure.log;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.List;

import static org.corfudb.infrastructure.log.StreamLogParams.RECORDS_PER_SEGMENT;

/**
 * ID of the log segment, which is composed of first and
 * last log address in the segment.
 *
 * Created by WenbinZhu on 2/6/20.
 */
@Slf4j
@EqualsAndHashCode
public class SegmentId implements Comparable<SegmentId> {

    // Start address inclusive.
    @Getter
    private long startAddress;

    // End address exclusive.
    @Getter
    private long endAddress;

    SegmentId(long startAddress, long endAddress) {
        Preconditions.checkArgument(
                startAddress < endAddress,
                "Invalid segment ID, start address %s not less than end address %s.",
                startAddress, endAddress);
        Preconditions.checkArgument(
                ((endAddress - startAddress)) % RECORDS_PER_SEGMENT == 0,
                "Invalid segment ID, number of addresses included NOT multiple of RECORDS_PER_SEGMENT");
        this.startAddress = startAddress;
        this.endAddress = endAddress;
    }

    /**
     * Returns true if this segment ID encloses the provided one.
     *
     * @param o other segment ID
     * @return true if address falls in the segment, false otherwise
     */
    boolean encloses(SegmentId o) {
        return startAddress <= o.startAddress && endAddress >= o.endAddress;
    }

    /**
     * Returns true if this segment ID is strictly less than the provided address.
     *
     * @param address address to compare
     * @return true if this segment ID is strictly less than the provided address
     */
    boolean lessThan(long address) {
        return endAddress <= address;
    }

    /**
     * Get the merged segment ID from a list of segment IDs which are supposed
     * to be consecutive.
     *
     * @param segmentIds a list of consecutive segment IDs
     * @return the merged segment ID
     */
    static SegmentId getMergedSegmentId(List<SegmentId> segmentIds) {
        if (segmentIds.isEmpty()) {
            throw new IllegalStateException("getMergedSegmentId: input segment ID list is empty!");
        }

        long startAddress = Long.MAX_VALUE;
        long endAddress = Long.MIN_VALUE;
        for (SegmentId segmentId : segmentIds) {
            startAddress = Math.min(startAddress, segmentId.startAddress);
            endAddress = Math.max(endAddress, segmentId.endAddress);
        }

        return new SegmentId(startAddress, endAddress);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return startAddress + "_" + endAddress;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(@Nonnull SegmentId o) {
        return Long.compare(startAddress, o.getStartAddress());
    }
}
