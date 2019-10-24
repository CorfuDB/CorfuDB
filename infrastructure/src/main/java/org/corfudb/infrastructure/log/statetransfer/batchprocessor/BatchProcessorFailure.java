package org.corfudb.infrastructure.log.statetransfer.batchprocessor;


import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.StateTransferException;

/**
 * A state transfer exception that is propagated to the caller of a batch processor,
 * after a batch processor failed to handle a batch transfer.
 */
@Getter
public class BatchProcessorFailure extends StateTransferException {

    public BatchProcessorFailure() {
        super();
    }

    public BatchProcessorFailure(Throwable cause) {
        super(cause);
    }

}
