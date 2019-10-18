package org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchTransferPlan;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferException;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferFailure;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class ProtocolBatchProcessor implements StateTransferBatchProcessor {


    public static final int MAX_RETRIES = 3;
    public static final Duration MAX_RETRY_TIMEOUT = Duration.ofSeconds(10);
    public static final float RANDOM_FACTOR_BACKOFF = 0.5f;

    @Getter
    private final ReadOptions readOptions = ReadOptions.builder()
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
    public CompletableFuture<Result<Long, StateTransferException>> transfer(BatchTransferPlan batchTransferPlan) {
        return readRecords(batchTransferPlan.getTransferAddresses())
                .thenApply(result -> result.flatMap(data -> writeRecords(data, streamLog)))
                .exceptionally(e -> Result.error(new StateTransferFailure(e)));
    }

    /**
     * Reads data entries by utilizing the replication protocol. If there are incomplete reads,
     * retry.
     *
     * @param addresses The list of addresses.
     * @return A result of reading records.
     */
    CompletableFuture<Result<List<LogData>, StateTransferException>> readRecords(List<Long> addresses) {

        return CompletableFuture.supplyAsync(() ->
                Result.of(() -> addressSpaceView.read(addresses, readOptions)))
                .thenApply(result -> result.flatMap(records ->
                        checkReadRecords(addresses, records)
                                .mapError(e -> new IncompleteDataReadException(e.getMissingAddresses())))
                ).thenCompose(result ->
                        {
                            CompletableFuture<Result<List<LogData>, StateTransferException>> newResult;

                            if (result.isError() && result.getError() instanceof IncompleteReadException) {
                                IncompleteReadException incompleteReadException =
                                        (IncompleteDataReadException) result.getError();
                                newResult =
                                        tryHandleIncompleteRead(incompleteReadException,
                                                new AtomicInteger(MAX_RETRIES));
                            } else if (result.isValue()) {
                                newResult = CompletableFuture.completedFuture(result
                                        .mapError(StateTransferException::new));
                            } else {
                                newResult = CompletableFuture
                                        .completedFuture(Result.error(new StateTransferFailure(result.getError())));
                            }
                            return newResult;
                        }
                );
    }

    /**
     * Handle possible read errors. Exponential backoff policy is utilized.
     *
     * @param incompleteReadException Exception that occurred during the read.
     * @param readRetries             Configurable number of retries for the current batch.
     * @return Future of the result of the retry.
     */
    CompletableFuture<Result<List<LogData>, StateTransferException>> tryHandleIncompleteRead
    (IncompleteReadException incompleteReadException, AtomicInteger readRetries) {

        try {
            return IRetry.build(ExponentialBackoffRetry.class, RetryExhaustedException.class, () -> {

                List<Long> addresses = Ordering.natural().sortedCopy(incompleteReadException.getMissingAddresses());

                Supplier<CompletableFuture<Result<List<LogData>, StateTransferException>>> pipeline =
                        () -> readRecords(addresses);

                // If a pipeline completed exceptionally, than return the error.
                CompletableFuture<Result<List<LogData>, StateTransferException>> pipelineFuture =
                        pipeline.get().handle((result, exception) -> {
                            if (Optional.ofNullable(exception).isPresent()) {
                                return Result.error(new StateTransferFailure(exception));
                            } else {
                                return result;
                            }
                        });
                // Join the result.
                Result<List<LogData>, StateTransferException> joinResult = pipelineFuture.join();
                if (joinResult.isError()) {
                    // If an error occurred, increment retries.
                    readRetries.incrementAndGet();

                    // If the instance of an error is the incomplete read, retry with exp. backoff
                    // if possible.
                    if (joinResult.getError() instanceof IncompleteReadException) {
                        if (readRetries.get() >= MAX_RETRIES) {
                            throw new RetryExhaustedException("Read retries are exhausted");
                        } else {
                            log.warn("Retried {} times", readRetries.get());
                            throw new RetryNeededException();
                        }
                    } else {
                        // Unhandled error, return.
                        return CompletableFuture.completedFuture(joinResult);
                    }
                } else {
                    // If the result is not an error, return.
                    return CompletableFuture.completedFuture(joinResult);
                }

            }).setOptions(retry -> {
                retry.setMaxRetryThreshold(MAX_RETRY_TIMEOUT);
                retry.setRandomPortion(RANDOM_FACTOR_BACKOFF);
            }).run();
            // Map to unrecoverable error if an interrupt has occurred or retries occurred.
        } catch (InterruptedException | RetryExhaustedException ie) {
            return CompletableFuture.completedFuture(
                    Result.error(new StateTransferFailure("Retries exhausted or interrupted")));
        }
    }

    /**
     * Sanity check after the read is performed. If the number of records is not equal to the
     * intended, return an incomplete read exception.
     *
     * @param addresses  Addresses that were read.
     * @param readResult A result of the read.
     * @return A result containing either exception or data.
     */
    Result<List<LogData>, IncompleteReadException> checkReadRecords(List<Long> addresses,
                                                                    Map<Long, ILogData> readResult) {
        List<Long> transferredAddresses =
                addresses.stream().filter(readResult::containsKey)
                        .collect(Collectors.toList());
        if (transferredAddresses.equals(addresses)) {
            return Result.of(() -> readResult.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> (LogData) entry.getValue()).collect(Collectors.toList()));
        } else {
            HashSet<Long> transferredSet = new HashSet<>(transferredAddresses);
            HashSet<Long> entireSet = new HashSet<>(addresses);
            return new Result<>(new ArrayList<>(),
                    new IncompleteReadException(Sets.difference(entireSet, transferredSet)));
        }
    }


    @Override
    public CompletableFuture<BatchResult> transfer(Batch batch) {
        return null;
    }
}
