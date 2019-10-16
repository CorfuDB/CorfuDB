package org.corfudb.infrastructure.log.statetransfer.batchprocessor.weightedbatchprocessor;

import lombok.Getter;


@Getter
public class QueueFullException extends WeightedRoundRobinBatchProcessorException {
    public QueueFullException(String message){
        super(message);
    }
}
