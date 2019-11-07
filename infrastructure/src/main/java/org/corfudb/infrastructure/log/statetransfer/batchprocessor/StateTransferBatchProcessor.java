package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.runtime.exceptions.OverwriteException;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

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
     * @param readBatch      The list of log entries as well as an optional
     *                       node they were read from.
     * @param streamlog      A stream log interface.
     * @param overwriteExceptionsAllowed A total number of overwrite exceptions allowed.
     * @return A transferBatchResponse, a final result of a transfer.
     */
    default TransferBatchResponse writeRecords(
            ReadBatch readBatch, StreamLog streamlog, AtomicInteger overwriteExceptionsAllowed) {
        List<Long> addresses = readBatch.getAddresses();
        Optional<String> destination = readBatch.getDestination();
        try {
            streamlog.append(readBatch.getData());
        } catch (OverwriteException owe) {
            // If the overwrite exceptions are no longer tolerated, rethrow.
            if (overwriteExceptionsAllowed.decrementAndGet() == 0) {
                throw owe;
            }
        }
        return TransferBatchResponse
                .builder()
                .transferBatchRequest(new TransferBatchRequest(addresses, destination))
                .status(SUCCEEDED)
                .build();
    }

}
