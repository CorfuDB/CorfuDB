package org.corfudb.infrastructure.log.statetransfer.transferbatchprocessor.weightedbatchprocessor;

import lombok.AllArgsConstructor;

import java.util.Set;

@AllArgsConstructor
public class QueueTimeOutException extends WeightedRoundRobinBatchProcessorException {
    private final Set<String> triedServers;
}
