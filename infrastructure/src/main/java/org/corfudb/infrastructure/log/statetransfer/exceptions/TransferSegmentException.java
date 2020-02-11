package org.corfudb.infrastructure.log.statetransfer.exceptions;

import org.corfudb.infrastructure.log.statetransfer.StateTransferException;

/**
 * A state transfer exception that is propagated to the caller once a transfer for one segment
 * has failed.
 */
public class TransferSegmentException extends StateTransferException {
    public TransferSegmentException(Throwable throwable) {
        super(throwable);
    }

    public TransferSegmentException() {
        super();
    }

    public TransferSegmentException(String message) {
        super(message);
    }
}
