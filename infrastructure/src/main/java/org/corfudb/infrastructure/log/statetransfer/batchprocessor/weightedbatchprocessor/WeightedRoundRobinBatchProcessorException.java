package org.corfudb.infrastructure.log.statetransfer.batchprocessor.weightedbatchprocessor;

import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferException;

public class WeightedRoundRobinBatchProcessorException extends StateTransferException {
    public WeightedRoundRobinBatchProcessorException(){}

    public WeightedRoundRobinBatchProcessorException(String message) {
        super(message);
    }

    public WeightedRoundRobinBatchProcessorException(String message, Throwable cause) {
        super(message, cause);
    }

    public WeightedRoundRobinBatchProcessorException(Throwable cause) {
        super(cause);
    }

}
