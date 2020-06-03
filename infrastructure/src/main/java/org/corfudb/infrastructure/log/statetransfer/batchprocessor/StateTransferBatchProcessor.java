package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.protocols.wireprotocol.ILogData;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest.TransferBatchType.DATA;
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

    default Set<Long> getNonTransferredAddresses(LogUnitClient client, ImmutableList<Long> addressesToTransfer) {
        if (addressesToTransfer.isEmpty()) {
            return new HashSet<>();
        }
        long start = addressesToTransfer.get(0);
        long end = addressesToTransfer.get(addressesToTransfer.size() - 1);
        Set<Long> knownAddresses =
                CFUtils.getUninterruptibly(client.requestKnownAddresses(start, end)).getKnownAddresses();
        return Sets.difference(new HashSet<>(addressesToTransfer), knownAddresses);
    }

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
                                readBatch.getDestinationNode().map(ImmutableList::of), DATA))
                        .status(SUCCEEDED)
                        .build());
                break;
            } catch (OverwriteException | TimeoutException | NetworkException e) {
                lastWriteException = Optional.of(e);
                Sleep.sleepUninterruptibly(writeSleepDuration);
                // Get all the addresses that were not written.
                Set<Long> nonWrittenAddresses = getNonTransferredAddresses(logUnitClient, addressesToTransfer);

                // If the delta is empty -> return with success.
                if (nonWrittenAddresses.isEmpty()) {
                    transferBatchResponse = Optional.of(TransferBatchResponse
                            .builder()
                            .transferBatchRequest(new TransferBatchRequest(addressesToTransfer,
                                    readBatch.getDestinationNode().map(ImmutableList::of), DATA))
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

    /**
     * Sanity check after the read is performed. If the number of records is not equal to the
     * intended, return with an error.
     *
     * @param addresses       All addresses that has to be read.
     * @param readResult      A result of a read operation.
     * @param destinationNode Optional source of records.
     * @return A read batch.
     */
    @VisibleForTesting
    default ReadBatch checkReadRecords(List<Long> addresses,
                                       Map<Long, ILogData> readResult,
                                       Optional<String> destinationNode) {
        List<Long> transferredAddresses =
                addresses.stream()
                        .filter(readResult::containsKey)
                        .collect(Collectors.toList());
        if (transferredAddresses.equals(addresses)) {
            List<LogData> logData = readResult.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> (LogData) entry.getValue()).collect(Collectors.toList());
            return ReadBatch.builder()
                    .data(logData)
                    .destinationNode(destinationNode)
                    .status(ReadBatch.ReadStatus.SUCCEEDED)
                    .build();
        } else {
            HashSet<Long> transferredSet = new HashSet<>(transferredAddresses);
            HashSet<Long> entireSet = new HashSet<>(addresses);
            List<Long> failedAddresses = Ordering
                    .natural()
                    .sortedCopy(Sets.difference(entireSet, transferredSet));
            return ReadBatch.builder()
                    .failedAddresses(failedAddresses)
                    .destinationNode(destinationNode)
                    .status(ReadBatch.ReadStatus.FAILED)
                    .build();
        }
    }

}
