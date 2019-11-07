package org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.StateTransferException;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.ReadBatchException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferBatchProcessorException;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
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
    private final Duration maxRetryTimeout = Duration.ofSeconds(10);
    @Default
    private final float randomFactorBackoff = 0.5f;
    @Default
    private final AtomicInteger maxOverwriteExceptions = new AtomicInteger(3);

    /**
     * Default read options for the replication protocol read.
     */
    @Getter
    private static ReadOptions readOptions = ReadOptions.builder()
            .waitForHole(true)
            .clientCacheable(false)
            .serverCacheable(false)
            .build();

    @Getter
    public final StreamLog streamLog;

    @Getter
    private final AddressSpaceView addressSpaceView;

    @Override
    public CompletableFuture<TransferBatchResponse> transfer(TransferBatchRequest transferBatchRequest) {
        return readRecords(transferBatchRequest, 0)
                .thenApply(records -> writeRecords(records, streamLog, maxOverwriteExceptions))
                .exceptionally(error -> TransferBatchResponse
                        .builder()
                        .status(FAILED)
                        .causeOfFailure(Optional.of(new StateTransferBatchProcessorException(error)))
                        .build()
                );
    }

    /**
     * Reads data entries by utilizing the replication protocol. If there are errors, retries.
     *
     * @param transferBatchRequest The transferBatchRequest of addresses to be read.
     * @param retries              The number of retries.
     * @return A result of reading records.
     */
    CompletableFuture<ReadBatch> readRecords(TransferBatchRequest transferBatchRequest, int retries) {
        return CompletableFuture
                .supplyAsync(() -> {
                            Supplier<Map<Long, ILogData>> readSupplier = () -> addressSpaceView
                                    .simpleProtocolRead(transferBatchRequest.getAddresses(), readOptions);

                            return Result
                                    .of(readSupplier)
                                    .mapError(StateTransferException::new);
                        }
                ).thenApply(readResult ->
                        readResult.map(records -> checkReadRecords(
                                transferBatchRequest.getAddresses(),
                                records,
                                transferBatchRequest.getDestination()
                        ))
                ).thenCompose(checkedReadResult -> {
                    if (checkedReadResult.isError()) {
                        return retryReadRecords(transferBatchRequest, retries);
                    } else {
                        ReadBatch readBatch = checkedReadResult.get();
                        if (readBatch.getStatus() == ReadBatch.ReadStatus.FAILED) {
                            return retryReadRecords(readBatch.createRequest(), retries);
                        } else {
                            return CompletableFuture.completedFuture(readBatch);
                        }
                    }
                });
    }

    /**
     * Handle read errors. Exponential backoff policy is utilized.
     *
     * @param transferBatchRequest TransferBatchRequest of addresses to be read.
     * @param retriesTried         Configurable number of retries for the current transferBatchRequest.
     * @return Future of the result of the retry.
     */
    CompletableFuture<ReadBatch> retryReadRecords
    (TransferBatchRequest transferBatchRequest, int retriesTried) {
        try {
            AtomicInteger readRetries = new AtomicInteger(retriesTried);
            return IRetry.build(ExponentialBackoffRetry.class, RetryExhaustedException.class, () -> {
                if (readRetries.incrementAndGet() >= maxReadRetries) {
                    throw new RetryExhaustedException("Read retries are exhausted");
                }

                // If a pipeline completed exceptionally, than return the error.
                CompletableFuture<ReadBatch> pipelineFuture =
                        readRecords(transferBatchRequest, readRetries.get())
                                .handle((result, err) -> Optional.ofNullable(err)
                                        .map(e -> ReadBatch.builder()
                                                .failedAddresses(transferBatchRequest.getAddresses())
                                                .destination(transferBatchRequest.getDestination())
                                                .causeOfFailure(Optional.of(new ReadBatchException(e)))
                                                .status(ReadBatch.ReadStatus.FAILED)
                                                .build())
                                        .orElse(result)
                                );

                ReadBatch readBatch = pipelineFuture.join();

                // If the result is an error, print an error message and retry.
                if (readBatch.getStatus() == ReadBatch.ReadStatus.FAILED) {
                    readBatch.getCauseOfFailure()
                            .ifPresent(e -> log.error("A read failed with {}.", e.getMessage()));
                    log.error("Retrying {} times.", readRetries.get());
                    throw new RetryNeededException();
                } else {
                    // If the result is not an error, return.
                    return CompletableFuture.completedFuture(readBatch);
                }

            }).setOptions(retry -> {
                retry.setMaxRetryThreshold(maxRetryTimeout);
                retry.setRandomPortion(randomFactorBackoff);
            }).run();

        } catch (InterruptedException | RetryExhaustedException exception) {
            log.error("Retries were exhausted.");
            CompletableFuture<ReadBatch> future = new CompletableFuture<>();
            future.completeExceptionally(exception);
            return future;
        }
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
    ReadBatch checkReadRecords(List<Long> addresses,
                               Map<Long, ILogData> readResult,
                               Optional<String> destination) {
        List<Long> transferredAddresses =
                addresses.stream()
                        .filter(readResult::containsKey)
                        .collect(Collectors.toList());
        if (transferredAddresses.equals(addresses)) {
            List<LogData> lodData = readResult.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> (LogData) entry.getValue()).collect(Collectors.toList());
            return ReadBatch.builder()
                    .data(lodData)
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
