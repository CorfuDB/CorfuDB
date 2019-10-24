package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.StateTransferException;

/**
 * An error that should be handled by the batch processor instance.
 */
@Getter
public class BatchProcessorError extends StateTransferException {

    public BatchProcessorError() {
        super();
    }

    public BatchProcessorError(Throwable cause) {
        super(cause);
    }
}
