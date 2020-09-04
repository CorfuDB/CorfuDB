package org.corfudb.infrastructure.log.statetransfer;


import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegment;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;

import java.util.Optional;

import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState;
import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.builder;

/**
 * An utility interface to create the instances of transfer segments.
 */
public interface TransferSegmentCreator {

    default TransferSegment createTransferSegment(long start,
                                                  long end,
                                                  SegmentState state) {
        // If a passed state is restored or transferred, create a new segment with a given state
        // and also compute the range.
        if (state == RESTORED || state == TRANSFERRED) {
            return TransferSegment
                    .builder()
                    .status(
                            builder()
                                    .segmentState(state)
                                    .build()
                    )
                    .startAddress(start)
                    .endAddress(end)
                    .logUnitServers(ImmutableList.of())
                    .build();
        }
        // If a state is not transferred, create a segment.
        else if(state == NOT_TRANSFERRED){
            return TransferSegment
                    .builder()
                    .status(
                            builder()
                                    .segmentState(state)
                                    .build()
                    )
                    .startAddress(start)
                    .endAddress(end)
                    .logUnitServers(ImmutableList.of())
                    .build();
        }
        // If a state is failed, create a transfer segment with a provided failure.
        else {
            return TransferSegment
                    .builder()
                    .status(builder()
                            .segmentState(state)
                            .causeOfFailure(Optional.of(new TransferSegmentException()))
                            .build()
                    )
                    .startAddress(start)
                    .endAddress(end)
                    .logUnitServers(ImmutableList.of())
                    .build();
        }
    }

    // Create a transfer segment status from a given state, total transferred and an optional failure.
    default TransferSegmentStatus createStatus(
            SegmentState state, Optional<TransferSegmentException> fail) {
        return builder().segmentState(state).causeOfFailure(fail).build();
    }

}
