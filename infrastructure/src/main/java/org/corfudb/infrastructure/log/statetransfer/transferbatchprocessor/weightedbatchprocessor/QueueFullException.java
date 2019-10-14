package org.corfudb.infrastructure.log.statetransfer.transferbatchprocessor.weightedbatchprocessor;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Set;

@AllArgsConstructor
@Getter
public class QueueFullException extends WeightedRoundRobinBatchProcessorException {
    private final Set<String> triedServers;
}
