package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.SUCCEEDED;

/**
 * An interface that every state transfer batch processor should implement.
 */
public interface StateTransferBatchProcessor {

    /**
     * Invoke a transfer given a transferBatchRequest and return a future of a transferBatchResponse.
     *
     * @param transferBatchRequest A request to transfer a batch of addresses.
     * @return A result of a transfer.
     */
    CompletableFuture<TransferBatchResponse> transfer(TransferBatchRequest transferBatchRequest);

    /**
     * Appends records to the stream log.
     *
     * @param readBatch The list of log entries as well as an optional
     *                  node they were read from.
     * @param streamlog A stream log interface.
     * @return A transferBatchResponse, a final result of a transfer.
     */
    default TransferBatchResponse writeRecords(ReadBatch readBatch, StreamLog streamlog) {
        List<Long> addresses = readBatch.getAddresses();

        Result<TransferBatchResponse, RuntimeException> resultOfWrite = Result.of(() -> {
            streamlog.append(readBatch.getData());
            return TransferBatchResponse
                    .builder()
                    .transferBatchRequest(new TransferBatchRequest(addresses, readBatch.getDestination()))
                    .status(SUCCEEDED)
                    .build();
        });
        if (resultOfWrite.isError()) {
            return TransferBatchResponse
                    .builder()
                    .transferBatchRequest(new TransferBatchRequest(addresses, readBatch.getDestination()))
                    .status(FAILED)
                    .build();
        }
        return resultOfWrite.get();
    }

}
