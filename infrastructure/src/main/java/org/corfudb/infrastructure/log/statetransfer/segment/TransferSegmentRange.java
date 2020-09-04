package org.corfudb.infrastructure.log.statetransfer.segment;

import java.util.List;

/**
 * An interface that represents a single or a split range of transfer segment.
 */
public interface TransferSegmentRange {
    /**
     * Turn range into a segment.
     *
     * @return A transfer segment
     */
    TransferSegment toTransferSegment();

    /**
     * Turn this range into a single range.
     *
     * @return A list of one or two single ranges
     */
    List<TransferSegmentRangeSingle> toSingle();

    /**
     * Update the current transfer segment range with a provided status.
     *
     * @param newStatus A new status
     * @return An updated transfer segment range
     */
    TransferSegmentRange updateStatus(TransferSegmentStatus newStatus);

    /**
     * Get the status of this transfer segment range.
     *
     * @return A transfer status
     */
    TransferSegmentStatus getStatus();
}
