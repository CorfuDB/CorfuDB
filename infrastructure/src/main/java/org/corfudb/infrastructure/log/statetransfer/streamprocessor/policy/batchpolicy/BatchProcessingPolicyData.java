package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.batchpolicy;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;

@AllArgsConstructor
@Getter
public class BatchProcessingPolicyData {
    private final Batch batch;
}
