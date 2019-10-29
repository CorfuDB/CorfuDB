package org.corfudb.infrastructure.log.statetransfer;

import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class GoodBatchProcessor implements StateTransferBatchProcessor, DelayedExecution {
    private final Random random = new Random();
    private final ScheduledExecutorService ec = Executors.newScheduledThreadPool(1);

    public final Optional<Long> delay;

    public GoodBatchProcessor(Optional<Long> delay) {
        this.delay = delay;
    }
    public GoodBatchProcessor(){
        this.delay = Optional.empty();
    }

    @Override
    public CompletableFuture<BatchResult> transfer(Batch batch) {
        CompletableFuture<BatchResult> exec = CompletableFuture
                .completedFuture(BatchResult
                        .builder()
                        .batch(batch)
                        .build()
                );
        return withDelayOf(() -> exec, delay.map(d -> (long)
                (random.nextFloat() * d)).orElse(0L), ec);
    }
}
