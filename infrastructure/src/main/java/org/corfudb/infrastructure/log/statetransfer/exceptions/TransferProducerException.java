package org.corfudb.infrastructure.log.statetransfer.exceptions;

import org.corfudb.infrastructure.log.statetransfer.StateTransferException;

/**
 * An exception that occurs on the producer task during parallel transfer.
 */
public class TransferProducerException extends StateTransferException {
    public TransferProducerException() {
        super();
    }

    public TransferProducerException(Throwable cause) {
        super(cause);
    }

    public TransferProducerException(String message) {
        super(message);
    }

    public TransferProducerException(String message, Throwable cause) {
        super(message, cause);
    }
}
