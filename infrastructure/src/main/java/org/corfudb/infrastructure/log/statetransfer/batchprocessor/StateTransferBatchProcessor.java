package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

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
        if (readBatch == null || readBatch.getAddresses().isEmpty()) {
            throw new IllegalArgumentException("The read batch is empty.");
        }
        ImmutableList<LogData> dataToTransfer = ImmutableList.copyOf(readBatch.getData());
        ImmutableList<Long> addressesToTransfer = ImmutableList.copyOf(readBatch.getAddresses());
        Optional<Exception> lastWriteException = Optional.empty();
        Optional<TransferBatchResponse> transferBatchResponse = Optional.empty();

        for (int i = 0; i < writeRetriesAllowed; i++) {
            List<LogData> remainingDataToWrite = readBatch.getData();
            try {
                CFUtils.getUninterruptibly(logUnitClient.writeRange(remainingDataToWrite),
                        OverwriteException.class,
                        TimeoutException.class,
                        NetworkException.class);
                transferBatchResponse = Optional.of(TransferBatchResponse
                        .builder()
                        .transferBatchRequest(new TransferBatchRequest(addressesToTransfer,
                                readBatch.getDestination()))
                        .status(SUCCEEDED)
                        .build());
                break;
            } catch (OverwriteException | TimeoutException | NetworkException e) {
                lastWriteException = Optional.of(e);
                Sleep.sleepUninterruptibly(writeSleepDuration);
                // Get all the addresses that were supposed to be written to a stream log.
                long start = addressesToTransfer.get(0);
                long end = addressesToTransfer.get(addressesToTransfer.size() - 1);
                Set<Long> knownAddresses = CFUtils
                        .getUninterruptibly(logUnitClient.requestKnownAddresses(start, end))
                        .getKnownAddresses();
                // Get all the addresses that were not written.
                Set<Long> nonWrittenAddresses =
                        Sets.difference(new HashSet<>(addressesToTransfer), knownAddresses);
                // If the delta is empty -> return with success.
                if (nonWrittenAddresses.isEmpty()) {
                    transferBatchResponse = Optional.of(TransferBatchResponse
                            .builder()
                            .transferBatchRequest(new TransferBatchRequest(addressesToTransfer,
                                    readBatch.getDestination()))
                            .status(SUCCEEDED)
                            .build());
                    break;
                }
                // Create a new read batch with the missing data and retry.
                ImmutableList<LogData> nonWrittenData = dataToTransfer
                        .stream()
                        .filter(data -> nonWrittenAddresses.contains(data.getGlobalAddress()))
                        .collect(ImmutableList.toImmutableList());
                readBatch = readBatch.toBuilder().data(nonWrittenData).build();
            } catch (RuntimeException re) {
                // If it's some other exception, e.g. IllegalArgumentException,
                // WrongEpochException -> Propagate it to the caller.
                throw new IllegalStateException(re);
            }
        }

        final Optional<Exception> finalWriteException = lastWriteException;
        String msg = "writeRecords: Retries are exhausted.";
        return transferBatchResponse.orElseThrow(
                () -> finalWriteException
                        .map(ex -> new RetryExhaustedException(msg, ex))
                        .orElse(new RetryExhaustedException(msg)));
    }

}
