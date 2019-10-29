package org.corfudb.infrastructure.log.statetransfer.streamprocessor;

import org.corfudb.infrastructure.log.statetransfer.StateTransferException;

/**
 * A state transfer exception that is propagated to the caller of a stream processor,
 * after a stream processor failed to handle a stream transfer.
 */
public class StreamProcessFailure extends StateTransferException {
    public StreamProcessFailure(Throwable throwable) {
        super(throwable);
    }

    public StreamProcessFailure() {
        super();
    }
}
