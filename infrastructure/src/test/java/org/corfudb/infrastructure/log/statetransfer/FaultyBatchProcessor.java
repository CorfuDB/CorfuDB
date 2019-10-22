package org.corfudb.infrastructure.log.statetransfer;

import lombok.Getter;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResultData;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFailure;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A faulty batch processor that fails every {@link #batchFailureOrder transfer}.
 */
@Getter
public class FaultyBatchProcessor implements StateTransferBatchProcessor, DelayedExecution {

    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    private final Optional<Long> delay;
    private final int batchFailureOrder;
    private final Random random = new Random();
    private final ScheduledExecutorService ec = Executors.newScheduledThreadPool(1);

    public FaultyBatchProcessor(int batchFailureOrder, Optional<Long> delay) {
        this.batchFailureOrder = batchFailureOrder;
        this.delay = delay;
    }

    @Override
    public CompletableFuture<BatchResult> transfer(Batch batch) {
        CompletableFuture<BatchResult> exec;
        if (totalProcessed.getAndIncrement() % batchFailureOrder == 0) {
            exec = CompletableFuture.completedFuture(
                    new BatchResult(Result.error(BatchProcessorFailure.builder().build())));
        } else {
            exec = CompletableFuture
                    .completedFuture(new BatchResult
                            (Result.ok(new BatchResultData((long) batch.getAddresses().size()))));
        }
        return withDelayOf(() -> exec, delay.map(d -> (long) (random.nextFloat() * d)).orElse(0L), ec);
    }
}
