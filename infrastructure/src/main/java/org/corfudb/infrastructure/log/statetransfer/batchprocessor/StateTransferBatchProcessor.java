package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.SUCCEEDED;

/**
 * An interface that every state transfer batch processor should implement.
 */
public interface StateTransferBatchProcessor {
    Logger log = LoggerFactory.getLogger(StateTransferBatchProcessor.class);
    /**
     * Invoke a transfer given a transferBatchRequest and return a future of a transferBatchResponse.
     *
     * @param transferBatchRequest A request to transfer a batch of addresses.
     * @return A result of a transfer.
     */
    CompletableFuture<TransferBatchResponse> transfer(TransferBatchRequest transferBatchRequest);

    /**
     * From the list of addressesToTransfer get all the addresses that were not yet transferred.
     * Propagate exceptions to the caller.
     *
     * @param client              Log unit client to the current node.
     * @param addressesToTransfer List of addresses to transfer.
     * @return A set of addresses that were not yet transferred.
     */
    default Set<Long> getNonTransferredAddresses(LogUnitClient client,
                                                 ImmutableList<Long> addressesToTransfer)
            throws TimeoutException {
        if (addressesToTransfer.isEmpty()) {
            return new HashSet<>();
        }
        long start = addressesToTransfer.get(0);
        long end = addressesToTransfer.get(addressesToTransfer.size() - 1);
        if (start > end) {
            throw new IllegalArgumentException("Entries are not in sequential order.");
        }
        Set<Long> knownAddresses =
                CFUtils.getUninterruptibly(client.requestKnownAddresses(start, end),
                        TimeoutException.class, NetworkException.class).getKnownAddresses();


        return Sets.difference(new HashSet<>(addressesToTransfer), knownAddresses);
    }

    /**
     * Create a read batch from the addresses that failed to be written
     * to the current log unit server.
     * @param dataToTransfer        A list of log data that was supposed to be written
     *                              to the current log unit server entirely.
     * @param nonWrittenAddresses   A list of addresses that are not present in the current
     *                              log unit server.
     * @return                      A new read batch.
     */
    default ReadBatch createReadBatchFromNotTransferredAddresses(List<LogData> dataToTransfer,
                                                                 Set<Long> nonWrittenAddresses) {
        if (nonWrittenAddresses.isEmpty()) {
            throw new IllegalArgumentException("nonWrittenAddresses should be not empty " +
                    "to create a new read batch.");
        }
        Comparator<LogData> addressComparator =
                Comparator.comparing(IMetadata::getGlobalAddress);
        // Create a new read batch with the missing data..
        ImmutableList<LogData> nonWrittenData = dataToTransfer
                .stream()
                .sorted(addressComparator)
                .filter(data -> nonWrittenAddresses.contains(data.getGlobalAddress()))
                .collect(ImmutableList.toImmutableList());
        return ReadBatch.builder().data(nonWrittenData).build();
    }

    /**
     * Write addresses to the current log unit server. Propagate exceptions to the caller.
     * @param readBatch                 A read batch that contains log unit data.
     * @param logUnitClient             A log unit client to the current node.
     * @param allAddressesToTransfer    A list of all addresses to transfer for the current batch.
     * @return                          A TransferBatchResponse if no exceptions occurred.
     */
    default TransferBatchResponse tryWrite(ReadBatch readBatch, LogUnitClient logUnitClient,
                                        List<Long> allAddressesToTransfer) throws TimeoutException {
        List<LogData> remainingDataToWrite = readBatch.getData();

        CFUtils.getUninterruptibly(logUnitClient.writeRange(remainingDataToWrite),
                OverwriteException.class,
                TimeoutException.class,
                NetworkException.class);

        return TransferBatchResponse
                .builder()
                .transferBatchRequest(new TransferBatchRequest(
                        allAddressesToTransfer,
                        readBatch.getDestinationNode().map(ImmutableList::of)))
                .status(SUCCEEDED)
                .build();
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
        Optional<Exception> lastException = Optional.empty();
        for (int i = 0; i < writeRetriesAllowed; i++) {
            try {
                if (lastException.isPresent()) {
                    Set<Long> notWrittenAddresses =
                            getNonTransferredAddresses(logUnitClient, addressesToTransfer);
                    // If we've written all the addresses, return a successful TransferBatchResponse.
                    if (notWrittenAddresses.isEmpty()) {
                        return TransferBatchResponse
                                .builder()
                                .transferBatchRequest(
                                        new TransferBatchRequest(
                                                addressesToTransfer,
                                        readBatch.getDestinationNode().map(ImmutableList::of)))
                                .status(SUCCEEDED)
                                .build();
                    }
                    // Create a new ReadBatch from the notWrittenAddresses and retry the workflow.
                    readBatch = createReadBatchFromNotTransferredAddresses(dataToTransfer,
                            notWrittenAddresses);
                }
                return tryWrite(readBatch, logUnitClient, addressesToTransfer);
            } catch (OverwriteException | TimeoutException | NetworkException e) {
                log.warn("writeRecords: tryWrite failed. Retrying {}/{}.", i,
                        writeRetriesAllowed, e);
                lastException = Optional.of(e);
                Sleep.sleepUninterruptibly(writeSleepDuration);
            } catch (RuntimeException e) {
                lastException = Optional.of(e);
                log.error("writeRecords: failed. Stop retrying.", e);
                break;
            }
        }
        // Write retries are exhausted. Throw a RetryExhaustedException.
        throw lastException.map(RetryExhaustedException::new)
                .orElse(new RetryExhaustedException());
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
