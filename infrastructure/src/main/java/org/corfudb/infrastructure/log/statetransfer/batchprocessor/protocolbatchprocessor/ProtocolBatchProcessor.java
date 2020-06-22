package org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
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
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

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
    public CompletableFuture<TransferBatchResponse> transfer(
            TransferBatchRequest transferBatchRequest) {
        return readRecords(transferBatchRequest)
                .thenApply(records ->
                        writeRecords(records, logUnitClient, maxWriteRetries, writeSleepDuration))
                .exceptionally(error -> TransferBatchResponse
                        .builder()
                        .transferBatchRequest(transferBatchRequest)
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

            for (int i = 0; i < maxReadRetries; i++) {
                try {
                    Map<Long, ILogData> records =
                            addressSpaceView.simpleProtocolRead(
                                    transferBatchRequest.getAddresses(),
                                    readOptions);
                    ReadBatch batch = checkReadRecords(
                            transferBatchRequest.getAddresses(),
                            records, Optional.empty());
                    if (batch.getStatus() == ReadBatch.ReadStatus.FAILED) {
                        throw new IllegalStateException("Some addresses failed to transfer: " +
                                batch.getFailedAddresses());
                    }
                    return batch;

                } catch (WrongEpochException e) {
                    log.warn("readRecords: encountered a wrong epoch exception on try {}: {}.",
                            i, e);
                    throw e;
                } catch (RuntimeException e) {
                    log.warn("readRecords: encountered an exception on try {}: {}.", i, e);
                    Sleep.sleepUninterruptibly(readSleepDuration);
                }
            }
            // If the retries are exhausted, throw ReadBatchException.
            throw new ReadBatchException("readRecords: read retries were exhausted");
        });
    }
}
