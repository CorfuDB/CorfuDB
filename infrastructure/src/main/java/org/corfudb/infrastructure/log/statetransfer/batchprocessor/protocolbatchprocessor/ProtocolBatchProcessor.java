package org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorError;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
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
import static org.corfudb.infrastructure.log.statetransfer.batch.BatchResult.FailureStatus.FAILED;

/**
 * A batch processor that transfers non-committed addresses one batch at a time
 * via a replication protocol.
 */
@Slf4j
@Builder
public class ProtocolBatchProcessor implements StateTransferBatchProcessor {

    /**
     * Configurations for the retry logic.
     */
    @Default
    private final int maxRetries = 3;
    @Default
    private final Duration maxRetryTimeout = Duration.ofSeconds(10);
    @Default
    private final float randomFactorBackoff = 0.5f;

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
    public CompletableFuture<BatchResult> transfer(Batch batch) {
        return readRecords(batch, 0)
                .thenApply(records -> writeRecords(records, streamLog))
                .exceptionally(error -> BatchResult
                        .builder()
                        .status(FAILED)
                        .destinationServer(batch.getDestination())
                        .addresses(batch.getAddresses())
                        .build()
                );
    }

    /**
     * Reads data entries by utilizing the replication protocol. If there are errors, retries.
     *
     * @param batch   The batch of addresses to be read.
     * @param retries The number of retries.
     * @return A result of reading records.
     */
    CompletableFuture<ReadBatch> readRecords(Batch batch, int retries) {
        return CompletableFuture
                .supplyAsync(() -> {
                            Supplier<Map<Long, ILogData>> readSupplier = () -> addressSpaceView
                                    .simpleProtocolRead(batch.getAddresses(), readOptions);

                            return Result
                                    .of(readSupplier)
                                    .mapError(BatchProcessorError::new);
                        }
                )
                .thenApply(readResult ->
                        readResult.map(records -> checkReadRecords(
                                batch.getAddresses(),
                                records,
                                batch.getDestination()
                        ))
                )
                .thenCompose(checkedReadResult -> {
                    if (checkedReadResult.isError()) {
                        return retryReadRecords(batch, retries);
                    } else {
                        ReadBatch readBatch = checkedReadResult.get();
                        if (readBatch.getStatus() == ReadBatch.FailureStatus.FAILED) {
                            return retryReadRecords(readBatch.toBatch(), retries);
                        } else {
                            return CompletableFuture.completedFuture(readBatch);
                        }
                    }
                });
    }

    /**
     * Handle read errors. Exponential backoff policy is utilized.
     *
     * @param batch        Batch of addresses to be read.
     * @param retriesTried Configurable number of retries for the current batch.
     * @return Future of the result of the retry.
     */
    CompletableFuture<ReadBatch> retryReadRecords
    (Batch batch, int retriesTried) {
        try {
            AtomicInteger readRetries = new AtomicInteger(retriesTried);
            return IRetry.build(ExponentialBackoffRetry.class, RetryExhaustedException.class, () -> {
                if (readRetries.incrementAndGet() >= maxRetries) {
                    throw new RetryExhaustedException("Read retries are exhausted");
                }

                Supplier<CompletableFuture<ReadBatch>> pipeline =
                        () -> readRecords(batch, readRetries.get());

                // If a pipeline completed exceptionally, than return the error.
                CompletableFuture<ReadBatch> pipelineFuture =
                        pipeline.get().handle((result, exception) -> {
                            if (Optional.ofNullable(exception).isPresent()) {
                                return ReadBatch.builder()
                                        .failedAddress(batch.getAddresses())
                                        .destination(batch.getDestination())
                                        .status(ReadBatch.FailureStatus.FAILED)
                                        .build();
                            } else {
                                return result;
                            }
                        });
                // Join the result.
                ReadBatch joinResult = pipelineFuture.join();

                if (joinResult.getStatus() == ReadBatch.FailureStatus.FAILED) {
                    throw new RetryNeededException();
                } else {
                    // If the result is not an error, return.
                    return CompletableFuture.completedFuture(joinResult);
                }

            }).setOptions(retry -> {
                retry.setMaxRetryThreshold(maxRetryTimeout);
                retry.setRandomPortion(randomFactorBackoff);
            }).run();
            // Map to batch processor failure if an interrupt has occurred
            // or the retries were exhausted.
        } catch (InterruptedException | RetryExhaustedException ie) {
            log.error("Retries were exhausted.");
            return CompletableFuture.completedFuture(ReadBatch
                    .builder()
                    .status(ReadBatch.FailureStatus.FAILED)
                    .build()
            );
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
                addresses.stream().filter(readResult::containsKey)
                        .collect(Collectors.toList());
        if (transferredAddresses.equals(addresses)) {
            List<LogData> lodData = readResult.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> (LogData) entry.getValue()).collect(Collectors.toList());
            return ReadBatch.builder()
                    .data(lodData)
                    .destination(destination)
                    .status(ReadBatch.FailureStatus.SUCCEEDED)
                    .build();
        } else {
            HashSet<Long> transferredSet = new HashSet<>(transferredAddresses);
            HashSet<Long> entireSet = new HashSet<>(addresses);
            List<Long> failedAddresses = Ordering
                    .natural()
                    .sortedCopy(Sets.difference(entireSet, transferredSet));
            return ReadBatch.builder()
                    .failedAddress(failedAddresses)
                    .destination(destination)
                    .status(ReadBatch.FailureStatus.FAILED)
                    .build();
        }
    }
}
