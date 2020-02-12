package org.corfudb.infrastructure.log.statetransfer.exceptions;

import org.corfudb.infrastructure.log.statetransfer.StateTransferException;

/**
 * A state transfer exception that is propagated to the caller
 * once a read of the batch of addresses has failed.
 */
public class ReadBatchException extends StateTransferException {
    public ReadBatchException(Throwable throwable) {
        super(throwable);
    }

    public ReadBatchException() {
        super();
    }

    public ReadBatchException(String message) {
        super(message);
    }
}
