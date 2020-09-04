package org.corfudb.infrastructure.log.statetransfer.segment;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;

import java.util.Optional;

/**
 * A data class that represents a status of a segment to be transferred.
 */
@Getter
@ToString
@EqualsAndHashCode
@Builder(toBuilder = true)
public class TransferSegmentStatus {
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
     * A state of a segment.
     */
    @Builder.Default
    public final SegmentState segmentState = SegmentState.NOT_TRANSFERRED;
    /**
     * An optional cause of failure for this segment.
     */
    @Builder.Default
    @EqualsAndHashCode.Exclude
    public final Optional<TransferSegmentException> causeOfFailure = Optional.empty();
}
