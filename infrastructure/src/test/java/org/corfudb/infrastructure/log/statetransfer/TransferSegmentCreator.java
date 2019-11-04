package org.corfudb.infrastructure.log.statetransfer;

import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegment;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.TransferSegmentFailure;

import java.util.Optional;


import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.*;

public interface TransferSegmentCreator {
    default TransferSegment createTransferSegment(long start,
                                                  long end,
                                                  SegmentState state) {
        if (state == RESTORED || state == TRANSFERRED) {
            return TransferSegment
                    .builder()
                    .status(
                            builder()
                                    .totalTransferred(end - start + 1)
                                    .segmentState(state)
                                    .build()
                    )
                    .startAddress(start)
                    .endAddress(end)
                    .build();
        } else {
            return TransferSegment
                    .builder()
                    .status(builder()
                                    .totalTransferred(0L)
                                    .segmentState(state)
                                    .causeOfFailure(Optional.of(new TransferSegmentFailure()))
                                    .build()
                    )
                    .startAddress(start)
                    .endAddress(end)
                    .build();
        }
    }

    default TransferSegmentStatus createStatus(SegmentState state, long total, Optional<TransferSegmentFailure> fail) {
        return builder().totalTransferred(total).segmentState(state).causeOfFailure(fail).build();
    }

}
