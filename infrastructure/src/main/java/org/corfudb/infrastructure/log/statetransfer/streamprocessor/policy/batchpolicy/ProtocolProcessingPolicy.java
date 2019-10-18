package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.batchpolicy;

import lombok.AllArgsConstructor;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;

import java.util.concurrent.CompletableFuture;

@AllArgsConstructor
public class ProtocolProcessingPolicy implements BatchProcessingPolicy {

    private final ProtocolBatchProcessor protocolBatchProcessor;
    @Override
    public CompletableFuture<BatchResult> applyPolicy(BatchProcessingPolicyData data) {
        return protocolBatchProcessor.transfer(data.getBatch());
    }
}
