package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.batchpolicy;

import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface BatchProcessingPolicy {

    CompletableFuture<BatchResult> applyPolicy(BatchProcessingPolicyData data);
}
