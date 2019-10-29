package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.infrastructure.log.statetransfer.batch.BatchResult.FailureStatus.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.batch.BatchResult.FailureStatus.SUCCEEDED;

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
     * @param readBatch The list of entries (data or garbage) as well as an optional
     *                  node they were read from.
     * @param streamlog An instance of stream log.
     * @return A batch result of a record append.
     */
    default BatchResult writeRecords(ReadBatch readBatch, StreamLog streamlog) {
        List<Long> addresses = readBatch.getAddresses();

        Result<BatchResult, RuntimeException> resultOfWrite = Result.of(() -> {
            streamlog.append(readBatch.getData());
            return BatchResult
                    .builder()
                    .batch(new Batch(addresses, readBatch.getDestination()))
                    .status(SUCCEEDED)
                    .build();
        });
        if (resultOfWrite.isError()) {
            return BatchResult
                    .builder()
                    .batch(new Batch(addresses, readBatch.getDestination()))
                    .status(FAILED)
                    .build();
        }
        return resultOfWrite.get();
    }

}
