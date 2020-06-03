package org.corfudb.infrastructure.log.statetransfer.exceptions;

import org.corfudb.infrastructure.log.statetransfer.StateTransferException;

/**
 * An exception that occurs on the consumer task during parallel transfer.
 */
public class TransferConsumerException extends StateTransferException {

    public TransferConsumerException() {
        super();
    }

    public TransferConsumerException(Throwable cause) {
        super(cause);
    }

    public TransferConsumerException(String message) {
        super(message);
    }

    public TransferConsumerException(String message, Throwable cause) {
        super(message, cause);
    }
}
