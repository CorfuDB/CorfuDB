package org.corfudb.infrastructure.log.statetransfer;

import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.util.CFUtils;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A transferBatchRequest processor that succeeds on every {@link #transfer(TransferBatchRequest)} call.
 */
public class SuccessfulBatchProcessor implements StateTransferBatchProcessor {
    private final ScheduledExecutorService ec = Executors.newScheduledThreadPool(1);

    public final Optional<Long> delay;

    public SuccessfulBatchProcessor(Optional<Long> delay) {
        this.delay = delay;
    }

    public SuccessfulBatchProcessor() {
        this.delay = Optional.empty();
    }

    @Override
    public CompletableFuture<TransferBatchResponse> transfer(TransferBatchRequest transferBatchRequest) {
        CompletableFuture<TransferBatchResponse> exec = CompletableFuture
                .completedFuture(TransferBatchResponse
                        .builder()
                        .transferBatchRequest(transferBatchRequest)
                        .build()
                );
        return CFUtils.runFutureAfter(() -> exec, ec, delay.orElse(0L), TimeUnit.MILLISECONDS);
    }
}
