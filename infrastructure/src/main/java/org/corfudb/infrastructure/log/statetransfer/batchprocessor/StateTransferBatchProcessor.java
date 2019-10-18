package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface StateTransferBatchProcessor {

    CompletableFuture<BatchResult> transfer(Batch batch);

    /**
     * Appends records to the stream log.
     *
     * @param dataEntries The list of entries (data or garbage).
     * @return A result of a record append.
     */
    default Result<Long, BatchProcessorFailure>
    writeRecords(List<LogData> dataEntries, StreamLog streamlog) {
        return Result.of(() -> {
            streamlog.append(dataEntries);
            return (long) dataEntries.size();
        }).mapError(e -> BatchProcessorFailure.builder().throwable(e).build());
    }
}
