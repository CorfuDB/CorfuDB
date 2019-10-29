package org.corfudb.infrastructure.log.statetransfer.streamprocessor;

import org.corfudb.infrastructure.log.statetransfer.StateTransferException;

/**
 * A state transfer exception that is propagated to the caller once a transfer for one segment
 * has failed.
 */
public class TransferSegmentFailure extends StateTransferException {
    public TransferSegmentFailure(Throwable throwable) {
        super(throwable);
    }

    public TransferSegmentFailure() {
        super();
    }
}
