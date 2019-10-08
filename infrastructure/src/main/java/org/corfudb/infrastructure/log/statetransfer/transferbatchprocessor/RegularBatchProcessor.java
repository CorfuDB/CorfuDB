package org.corfudb.infrastructure.log.statetransfer.transferbatchprocessor;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.exceptions.IncompleteDataReadException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.IncompleteReadException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.RejectedAppendException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.RejectedDataException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferFailure;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.Sleep;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.corfudb.runtime.exceptions.OverwriteCause.*;
import static org.corfudb.runtime.view.Address.*;

@Slf4j
public class RegularBatchProcessor {


    public static final int MAX_RETRIES = 3;
    public static final Duration MAX_RETRY_TIMEOUT = Duration.ofSeconds(10);
    public static final float RANDOM_FACTOR_BACKOFF = 0.5f;

    @Getter
    public final StreamLog streamLog;

    @Getter
    private final AddressSpaceView addressSpaceView;

    @Getter
    private final ReadOptions readOptions = ReadOptions.builder()
            .waitForHole(true)
            .clientCacheable(false)
            .serverCacheable(false)
            .build();

    public RegularBatchProcessor(@NonNull StreamLog streamLog,
                                 @NonNull AddressSpaceView addressSpaceView) {
        this.streamLog = streamLog;
        this.addressSpaceView = addressSpaceView;
    }

    /**
     * Get the error handling function depending on the type of error occurred.
     *
     * @param exception Read or write exception.
     * @return A function that produces a future that handles the errors depending on their type.
     */

    Supplier<CompletableFuture<Result<Long, StateTransferException>>> getErrorHandlingPipeline(StateTransferException exception) {
        if (exception instanceof IncompleteReadException) {
            IncompleteReadException incompleteReadException = (IncompleteDataReadException) exception;
            List<Long> missingAddresses =
                    Ordering.natural().sortedCopy(incompleteReadException.getMissingAddresses());
            return () -> transfer(missingAddresses);

        } else {
            return () -> CompletableFuture.completedFuture
                    (Result.error(new StateTransferFailure("Unhandled error pipeline")));
        }
    }

    /**
     * Read data -> write data.
     *
     * @param addresses A batch of consecutive addresses.
     * @return Result of an empty value if a pipeline executes correctly;
     * a result containing the first encountered error otherwise.
     */
    public CompletableFuture<Result<Long, StateTransferException>> transfer(List<Long> addresses) {
        return CompletableFuture.supplyAsync(() -> readRecords(addresses))
                .thenApply(records -> records.flatMap(this::writeRecords));
    }


    /**
     * Handle errors that resulted during the transfer.
     *
     * @param transferResult Result of the transfer with a maximum transferred address or an exception.
     * @param retries        Number of retries during the error handling.
     * @return Future of the result of the transfer, containing maximum transferred address or
     * an exception.
     */
    public CompletableFuture<Result<Long, StateTransferException>> handlePossibleTransferFailures
    (Result<Long, StateTransferException> transferResult, AtomicInteger retries) {
        if (transferResult.isError()) {
            StateTransferException error = transferResult.getError();

            if (error instanceof IncompleteReadException) {
                IncompleteReadException incompleteReadException = (IncompleteReadException) error;

                return handlePossibleTransferFailures(tryHandleIncompleteRead(incompleteReadException, retries)
                        .join(), retries);

            } else if (error instanceof RejectedDataException) {
                log.trace("The data has been written previously.");
                RejectedDataException rejectedDataException = (RejectedDataException) error;
                List<LogData> rejectedEntries = rejectedDataException.getDataEntries();
                Optional<Long> maxWrittenAddress =
                        rejectedEntries.stream()
                                .map(IMetadata::getGlobalAddress)
                                .max(Comparator.comparing(Long::valueOf));

                return CompletableFuture.completedFuture
                        (Result.ok(maxWrittenAddress.orElse(NON_ADDRESS)));
            } else {
                Result<Long, StateTransferException> stateTransferFailureResult =
                        transferResult.mapError(StateTransferFailure::new);
                return CompletableFuture.completedFuture(stateTransferFailureResult);
            }
        } else {
            return CompletableFuture.completedFuture(transferResult);
        }
    }


    /**
     * Handle possible read errors. Exponential backoff policy is utilized.
     *
     * @param incompleteReadException Exception that occurred during the read.
     * @param readRetries             Configurable number of retries for the current batch.
     * @return Future of the result of the retry.
     */
    CompletableFuture<Result<Long, StateTransferException>> tryHandleIncompleteRead
    (IncompleteReadException incompleteReadException, AtomicInteger readRetries) {

        try {
            return IRetry.build(ExponentialBackoffRetry.class, RetryExhaustedException.class, () -> {

                // Get a pipeline function.
                Supplier<CompletableFuture<Result<Long, StateTransferException>>> pipeline =
                        getErrorHandlingPipeline(incompleteReadException);

                // If a pipeline completed exceptionally, than return the error.
                CompletableFuture<Result<Long, StateTransferException>> pipelineFuture =
                        pipeline.get().handle((result, exception) -> {
                            if (Optional.ofNullable(exception).isPresent()) {
                                return Result.error(new StateTransferFailure(exception));
                            } else {
                                return result;
                            }
                        });

                // Join the result.
                Result<Long, StateTransferException> joinResult = pipelineFuture.join();
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
     * intended one, return an incomplete read exception.
     *
     * @param addresses  Addresses that were read.
     * @param readResult A result of the read.
     * @return A result containing either exception or data.
     */
    Result<List<LogData>, IncompleteReadException> handleRead(List<Long> addresses,
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


    /**
     * Appends records to the stream log.
     *
     * @param dataEntries The list of entries (data or garbage).
     * @return A result of a record append, containing the max written address.
     */
    Result<Long, StateTransferException> writeRecords(List<LogData> dataEntries) {

        Result<Long, RuntimeException> result = Result.of(() -> {
            streamLog.append(dataEntries);
            Optional<Long> maxWrittenAddress =
                    dataEntries.stream()
                            .map(IMetadata::getGlobalAddress)
                            .max(Comparator.comparing(Long::valueOf));
            // Should be present as we've checked it during the previous stages.
            return maxWrittenAddress.orElse(NON_ADDRESS);
        });

        // If it's an overwrite exception with the same data cause, map to RejectedDataException,
        // otherwise map to StateTransferFailure.
        if (result.isError()) {
            RuntimeException error = result.getError();

            if (error instanceof OverwriteException &&
                    ((OverwriteException) error).getOverWriteCause().equals(SAME_DATA)) {

                return result.mapError(x -> new RejectedDataException(dataEntries));
            } else {
                return result.mapError(x -> new StateTransferFailure(x.getMessage()));
            }
        }

        return result.mapError(x -> new StateTransferException());
    }

    /**
     * Reads data entries by utilizing the replication protocol.
     *
     * @param addresses The list of addresses.
     * @return A result of reading records.
     */
    Result<List<LogData>, StateTransferException> readRecords(List<Long> addresses) {
        log.info("Reading data for addresses: {}", addresses);

        // Catch all possible unexpected failures.
        Result<Map<Long, ILogData>, StateTransferException> readResult =
                Result.of(() -> addressSpaceView.read(addresses, readOptions))
                .mapError(StateTransferFailure::new);
        return readResult.flatMap(result ->
                handleRead(addresses, result)
                        .mapError(e -> new IncompleteDataReadException(e.getMissingAddresses())));
    }
}
