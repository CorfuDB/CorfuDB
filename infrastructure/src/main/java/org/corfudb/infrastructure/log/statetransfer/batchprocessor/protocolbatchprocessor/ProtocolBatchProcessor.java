package org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.ReadBatchException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferBatchProcessorException;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static lombok.Builder.Default;
import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.FAILED;

/**
 * A transferBatchRequest processor that transfers non-committed addresses one transferBatchRequest at a time
 * via a replication protocol. A non-committed address is the one that belongs to the
 * part of an address space that is inconsistent.
 */
@Slf4j
@Builder
public class ProtocolBatchProcessor implements StateTransferBatchProcessor {

    /**
     * Configurations for the retry logic.
     */
    @Default
    private final int maxReadRetries = 3;
    @Default
    private final Duration readSleepDuration = Duration.ofMillis(300);
    @Default
    private final int maxWriteRetries = 3;
    @Default
    private final Duration writeSleepDuration = Duration.ofMillis(300);
    /**
     * Default read options for the replication protocol read.
     */
    @Getter
    private final ReadOptions readOptions = ReadOptions.builder()
            .waitForHole(true)
            .clientCacheable(false)
            .serverCacheable(false)
            .build();

    @Getter
    private final LogUnitClient logUnitClient;

    @Getter
    private final AddressSpaceView addressSpaceView;

    @Override
    public CompletableFuture<TransferBatchResponse> transfer(TransferBatchRequest transferBatchRequest) {
        return readRecords(transferBatchRequest)
                .thenApply(records ->
                        writeRecords(records, logUnitClient, maxWriteRetries, writeSleepDuration))
                .exceptionally(error -> TransferBatchResponse
                        .builder()
                        .status(FAILED)
                        .causeOfFailure(Optional.of(new StateTransferBatchProcessorException(
                                "Failed batch: " + transferBatchRequest, error)))
                        .build()
                );
    }

    /**
     * Reads data entries by utilizing the replication protocol. If there are errors, retry.
     * If the wrong epoch exception occurs, fail immediately, so that we can retry the workflow
     * earlier.
     *
     * @param transferBatchRequest The transferBatchRequest of addresses to be read.
     * @return A result of reading records.
     */
    @VisibleForTesting
    CompletableFuture<ReadBatch> readRecords(TransferBatchRequest transferBatchRequest) {
        return CompletableFuture.supplyAsync(() -> {

            Optional<ReadBatchException> latestReadException = Optional.empty();
            Optional<ReadBatch> latestReadBatch = Optional.empty();
            for (int i = 0; i < maxReadRetries; i++) {
                try {
                    Map<Long, ILogData> records =
                            addressSpaceView.simpleProtocolRead(transferBatchRequest.getAddresses(), readOptions);
                    ReadBatch batch = checkReadRecords(transferBatchRequest.getAddresses(),
                            records, transferBatchRequest.getDestination());
                    latestReadBatch = Optional.of(batch);
                    if (batch.getStatus() == ReadBatch.ReadStatus.FAILED) {
                        throw new IllegalStateException("Some addresses failed to transfer: " +
                                batch.getFailedAddresses());
                    } else {
                        latestReadException = Optional.empty();
                        break;
                    }

                } catch (WrongEpochException e) {
                    log.warn("readRecords: encountered a wrong epoch exception on try {}: ", i, e);
                    latestReadException = Optional.of(new ReadBatchException(e));
                    break;
                } catch (RuntimeException e) {
                    log.warn("readRecords: encountered an exception on try {}: ", i, e);
                    latestReadException = Optional.of(new ReadBatchException(e));
                    Sleep.sleepUninterruptibly(readSleepDuration);
                }
            }

            if (latestReadException.isPresent()) {
                throw latestReadException.get();
            } else {
                return latestReadBatch.get();
            }
        });
    }

    /**
     * Sanity check after the read is performed. If the number of records is not equal to the
     * intended, return with an error.
     *
     * @param addresses   All addresses that has to be read.
     * @param readResult  A result of a read operation.
     * @param destination An optional destination of the source of records.
     * @return A read batch.
     */
    @VisibleForTesting
    ReadBatch checkReadRecords(List<Long> addresses,
                               Map<Long, ILogData> readResult,
                               Optional<String> destination) {
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
                    .destination(destination)
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
                    .destination(destination)
                    .status(ReadBatch.ReadStatus.FAILED)
                    .build();
        }
    }
}
