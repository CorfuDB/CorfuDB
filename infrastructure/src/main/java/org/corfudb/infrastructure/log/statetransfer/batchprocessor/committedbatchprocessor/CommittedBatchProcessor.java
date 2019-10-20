package org.corfudb.infrastructure.log.statetransfer.batchprocessor.committedbatchprocessor;

import lombok.Getter;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.runtime.clients.LogUnitClient;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A batch processor that transfers globally committed addresses
 * one batch a time in an optimized fashion.
 */
public class CommittedBatchProcessor implements StateTransferBatchProcessor {

    @Getter
    private final StreamLog streamLog;

    @Getter
    private final Map<String, LogUnitClient> clientMap;

    public CommittedBatchProcessor(StreamLog streamLog,
                                   Map<String, LogUnitClient> clientMap) {
        this.streamLog = streamLog;
        this.clientMap = clientMap;
    }

    @Override
    public CompletableFuture<BatchResult> transfer(Batch batch) {
        throw new IllegalStateException("Not implemented");
    }
}
