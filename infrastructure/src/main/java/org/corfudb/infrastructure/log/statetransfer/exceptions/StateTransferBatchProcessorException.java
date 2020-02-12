package org.corfudb.infrastructure.log.statetransfer.exceptions;

import org.corfudb.infrastructure.log.statetransfer.StateTransferException;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;

/**
 * A state transfer exception that is propagated to the caller
 * once a transfer of one {@link TransferBatchRequest} has failed.
 */
public class StateTransferBatchProcessorException extends StateTransferException {
    public StateTransferBatchProcessorException(Throwable throwable) {
        super(throwable);
    }

    public StateTransferBatchProcessorException() {
        super();
    }

    public StateTransferBatchProcessorException(String message) {
        super(message);
    }

    public StateTransferBatchProcessorException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
