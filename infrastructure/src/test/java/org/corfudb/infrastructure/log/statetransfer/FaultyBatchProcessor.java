package org.corfudb.infrastructure.log.statetransfer;

import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.util.CFUtils;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.FAILED;

/**
 * A faulty batch processor that fails every {@link #batchFailureOrder transfer}.
 */
@Getter
public class FaultyBatchProcessor implements StateTransferBatchProcessor {

    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    private final Optional<Long> delay;
    private final int batchFailureOrder;
    private final Random random = new Random();
    private final ScheduledExecutorService ec = Executors.newScheduledThreadPool(1);

    public FaultyBatchProcessor(int batchFailureOrder, Optional<Long> delay) {
        this.batchFailureOrder = batchFailureOrder;
        this.delay = delay;
    }

    public FaultyBatchProcessor(int batchFailureOrder) {
        this.batchFailureOrder = batchFailureOrder;
        this.delay = Optional.empty();
    }


    @Override
    public CompletableFuture<TransferBatchResponse> transfer(TransferBatchRequest transferBatchRequest) {
        CompletableFuture<TransferBatchResponse> exec;
        if (totalProcessed.incrementAndGet() % batchFailureOrder == 0) {
            exec = CompletableFuture.completedFuture(TransferBatchResponse.builder().status(FAILED).build());
        } else {
            exec = CompletableFuture
                    .completedFuture(TransferBatchResponse
                            .builder()
                            .transferBatchRequest(transferBatchRequest)
                            .build()
                    );
        }
        return CFUtils.runFutureAfter(() -> exec, ec, delay.orElse(0L), TimeUnit.MILLISECONDS);
    }
}
