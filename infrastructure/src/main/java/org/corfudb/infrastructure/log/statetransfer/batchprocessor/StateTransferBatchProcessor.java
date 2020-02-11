package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

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
     * Appends records to the stream log. If a log throws an exception during an append, wait and retry.
     *
     * @param readBatch           The non-empty batch of log entries as well as an optional
     *                            node they were read from.
     * @param logUnitClient       A log unit client.
     * @param writeRetriesAllowed A total number of write retries allowed in case of an exception.
     * @param writeSleepDuration  A duration between retries.
     * @return A transferBatchResponse, a final result of a transfer.
     */
    default TransferBatchResponse writeRecords(ReadBatch readBatch, LogUnitClient logUnitClient,
                                               int writeRetriesAllowed, Duration writeSleepDuration) {
        List<Long> totalAddressesInRequest = readBatch.getAddresses();
        Optional<String> destination = readBatch.getDestination();
        Optional<Exception> lastWriteException = Optional.empty();
        Optional<TransferBatchResponse> transferBatchResponse = Optional.empty();

        for (int i = 0; i < writeRetriesAllowed; i++) {
            List<LogData> remainingDataToWrite = readBatch.getData();
            try {
                boolean writeSucceeded = logUnitClient.writeRange(remainingDataToWrite).join();
                if (!writeSucceeded) {
                    throw new IllegalStateException("Failed to write to a log unit server.");
                } else {
                    lastWriteException = Optional.empty();
                    transferBatchResponse = Optional.of(TransferBatchResponse
                            .builder()
                            .transferBatchRequest(new TransferBatchRequest(totalAddressesInRequest, destination))
                            .status(SUCCEEDED)
                            .build());
                    break;
                }
            } catch (Exception e) {
                lastWriteException = Optional.of(e);
                Sleep.sleepUninterruptibly(writeSleepDuration);
                List<Long> remainingAddressesToWrite = readBatch.getAddresses();
                // Get all the addresses that were supposed to be written to a stream log.
                long start = remainingAddressesToWrite.get(0);
                long end = remainingAddressesToWrite.get(remainingAddressesToWrite.size() - 1);
                Set<Long> knownAddresses =
                        logUnitClient.requestKnownAddresses(start, end).join().getKnownAddresses();
                // Get all the addresses that were not written.
                Set<Long> nonWrittenAddresses =
                        Sets.difference(new HashSet<>(remainingAddressesToWrite), knownAddresses);
                // Create a new read batch with the missing data and retry.
                ImmutableList<LogData> nonWrittenData = readBatch.getData()
                        .stream()
                        .filter(data -> nonWrittenAddresses.contains(data.getGlobalAddress()))
                        .collect(ImmutableList.toImmutableList());
                readBatch = readBatch.toBuilder().data(nonWrittenData).build();
            }
        }

        if (lastWriteException.isPresent()) {
            throw new IllegalStateException(lastWriteException.get());
        } else if (transferBatchResponse.isPresent()) {
            return transferBatchResponse.get();
        } else {
            throw new IllegalStateException("Exhausted retries writing to the log unit server.");
        }
    }

}
