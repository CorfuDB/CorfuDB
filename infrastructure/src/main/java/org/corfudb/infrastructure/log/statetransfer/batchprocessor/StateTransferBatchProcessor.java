package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * An interface that every batch processor should implement.
 */
public interface StateTransferBatchProcessor {

    /**
     * Transfer a batch and give back a future of a batch result.
     *
     * @param batch A batch to transfer.
     * @return A future of a batch result.
     */
    CompletableFuture<BatchResult> transfer(Batch batch);

    /**
     * Appends records to the stream log.
     *
     * @param dataEntries The list of entries (data or garbage).
     * @return A result of a record append, with a number of total written addresses or a failure.
     */
    default Result<Long, BatchProcessorFailure>
    writeRecords(List<LogData> dataEntries, StreamLog streamlog) {
        return Result.of(() -> {
            streamlog.append(dataEntries);
            return (long) dataEntries.size();
        }).mapError(e ->
                BatchProcessorFailure.builder()
                        .addresses(dataEntries.stream().map(IMetadata::getGlobalAddress).collect(Collectors.toList()))
                        .throwable(e).build());
    }
}
