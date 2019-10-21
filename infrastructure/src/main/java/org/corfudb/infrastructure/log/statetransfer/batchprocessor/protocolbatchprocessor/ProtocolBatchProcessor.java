package org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResultData;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorError;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFailure;
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

/**
 * A batch processor that transfers non-committed addresses one batch at a time
 * via a replication protocol.
 */
@Slf4j
public class ProtocolBatchProcessor implements StateTransferBatchProcessor {

    /**
     * Configurations for the retry logic.
     */
    private static final int MAX_RETRIES = 3;
    private static final Duration MAX_RETRY_TIMEOUT = Duration.ofSeconds(10);
    private static final float RANDOM_FACTOR_BACKOFF = 0.5f;

    /**
     *  Default read options for the replication protocol read.
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

    public ProtocolBatchProcessor(StreamLog streamLog, AddressSpaceView addressSpaceView) {
        this.streamLog = streamLog;
        this.addressSpaceView = addressSpaceView;
    }

    @Override
    public CompletableFuture<BatchResult> transfer(Batch batch) {
        CompletableFuture<Result<Long, BatchProcessorFailure>> protocolTransfer =
                readRecords(batch.getAddresses(), 0)
                        .thenApply(records ->
                                records.flatMap(recs ->
                                        writeRecords(recs, streamLog)));
        return protocolTransfer
                .thenApply(result ->
                        new BatchResult(result.map(BatchResultData::new)
                                .mapError(e -> BatchProcessorFailure.builder()
                                        .throwable(e).build())))
                .exceptionally(failure ->
                        new BatchResult(Result.error(
                                BatchProcessorFailure.builder()
                                        .throwable(failure).build())));
    }

    /**
     * Reads data entries by utilizing the replication protocol. If there are errors, retries.
     *
     * @param addresses The list of addresses.
     * @return A result of reading records.
     */
    CompletableFuture<Result<List<LogData>, BatchProcessorFailure>> readRecords(List<Long> addresses,
                                                                                int retries) {
        return CompletableFuture.supplyAsync(() ->
                Result.of(() -> addressSpaceView.simpleProtocolRead(addresses, readOptions))
                        .mapError(BatchProcessorError::new))
                .thenApply(readResult ->
                        readResult
                                .flatMap(records -> checkReadRecords(addresses, records)))
                .thenCompose(checkedReadResult -> {
                    if (checkedReadResult.isError()) {
                        return retryReadRecords(checkedReadResult.getError().getAddresses(),
                                retries);
                    } else {
                        return CompletableFuture.completedFuture(checkedReadResult
                                .mapError(e -> BatchProcessorFailure
                                        .builder().throwable(e).build()));
                    }
                });
    }

    /**
     * Handle read errors. Exponential backoff policy is utilized.
     *
     * @param addresses   Addresses to be retries.
     * @param retriesTried Configurable number of retries for the current batch.
     * @return Future of the result of the retry.
     */
    CompletableFuture<Result<List<LogData>, BatchProcessorFailure>> retryReadRecords
    (List<Long> addresses, int retriesTried) {

        try {
            AtomicInteger readRetries = new AtomicInteger(retriesTried);
            return IRetry.build(ExponentialBackoffRetry.class, RetryExhaustedException.class, () -> {
                if(readRetries.incrementAndGet() >= MAX_RETRIES){
                    throw new RetryExhaustedException("Read retries are exhausted");
                }
                Supplier<CompletableFuture<Result<List<LogData>, BatchProcessorFailure>>> pipeline =
                        () -> readRecords(addresses, readRetries.get());

                // If a pipeline completed exceptionally, than return the error.
                CompletableFuture<Result<List<LogData>, BatchProcessorFailure>> pipelineFuture =
                        pipeline.get().handle((result, exception) -> {
                            if (Optional.ofNullable(exception).isPresent()) {
                                return Result.error(BatchProcessorFailure.builder().throwable(exception).build());
                            } else {
                                return result;
                            }
                        });
                // Join the result.
                Result<List<LogData>, BatchProcessorFailure> joinResult = pipelineFuture.join();
                if (joinResult.isError()) {
                    throw new RetryNeededException();
                } else {
                    // If the result is not an error, return.
                    return CompletableFuture.completedFuture(joinResult);
                }

            }).setOptions(retry -> {
                retry.setMaxRetryThreshold(MAX_RETRY_TIMEOUT);
                retry.setRandomPortion(RANDOM_FACTOR_BACKOFF);
            }).run();
            // Map to batch processor failure if an interrupt has occurred
            // or the retries were exhausted.
        } catch (InterruptedException | RetryExhaustedException ie) {
            return CompletableFuture.completedFuture(
                    Result.error(BatchProcessorFailure.builder().throwable(ie).build()));
        }
    }

    /**
     * Sanity check after the read is performed. If the number of records is not equal to the
     * intended, return with an error.
     *
     * @param addresses  All addresses that has to be read.
     * @param readResult A result of a read operation.
     * @return A result containing either an error or data.
     */
    Result<List<LogData>, BatchProcessorError> checkReadRecords(List<Long> addresses,
                                                                Map<Long, ILogData> readResult) {
        List<Long> transferredAddresses =
                addresses.stream().filter(readResult::containsKey)
                        .collect(Collectors.toList());
        if (transferredAddresses.equals(addresses)) {
            return Result.ok(readResult.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> (LogData) entry.getValue()).collect(Collectors.toList()));
        } else {
            HashSet<Long> transferredSet = new HashSet<>(transferredAddresses);
            HashSet<Long> entireSet = new HashSet<>(addresses);
            return Result.error(
                    new BatchProcessorError
                            (Ordering.natural().sortedCopy(Sets.difference(entireSet, transferredSet))));
        }
    }
}
