package org.corfudb.infrastructure.log.statetransfer.transferbatchprocessor.weightedbatchprocessor;

public class WeightedRoundRobinBatchProcessorException extends RuntimeException{
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
