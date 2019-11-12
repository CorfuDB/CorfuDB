package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
     * @param readBatch           The non-empty batch of log entries as well as an optional
     *                            node they were read from.
     * @param streamlog           A stream log interface.
     * @param writeRetriesAllowed A total number of write retries allowed in case of an exception.
     * @param writeSleepDuration  A duration between retries.
     * @return A transferBatchResponse, a final result of a transfer.
     */
    default TransferBatchResponse writeRecords(
            ReadBatch readBatch, StreamLog streamlog,
            AtomicInteger writeRetriesAllowed, Duration writeSleepDuration) {
        List<Long> addresses = readBatch.getAddresses();
        Optional<String> destination = readBatch.getDestination();
        try {
            streamlog.append(readBatch.getData());
        } catch (Exception e) {
            // If the exceptions are no longer tolerated, rethrow.
            if (writeRetriesAllowed.decrementAndGet() == 0) {
                throw e;
            } else {
                Sleep.sleepUninterruptibly(writeSleepDuration);
                // Get all the addresses that were supposed to be written to a stream log.
                long start = addresses.get(0);
                long end = addresses.get(addresses.size() - 1);
                Set<Long> knownAddresses = streamlog.getKnownAddressesInRange(start, end);
                // Get all the addresses that were not written.
                Set<Long> nonWrittenAddresses =
                        Sets.difference(new HashSet<>(addresses), knownAddresses);
                // Create a new read batch with the missing data and retry.
                ImmutableList<LogData> nonWrittenData = readBatch.getData()
                        .stream()
                        .filter(data -> nonWrittenAddresses.contains(data.getGlobalAddress()))
                        .collect(ImmutableList.toImmutableList());
                ReadBatch newReadBatch = readBatch.toBuilder().data(nonWrittenData).build();
                // Try writing the records and also preserve the the original request.
                return writeRecords(newReadBatch, streamlog, writeRetriesAllowed, writeSleepDuration)
                        .toBuilder()
                        .transferBatchRequest(new TransferBatchRequest(addresses, destination))
                        .build();
            }
        }
        return TransferBatchResponse
                .builder()
                .transferBatchRequest(new TransferBatchRequest(addresses, destination))
                .status(SUCCEEDED)
                .build();
    }

}
