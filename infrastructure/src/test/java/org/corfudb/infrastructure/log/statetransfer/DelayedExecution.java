package org.corfudb.infrastructure.log.statetransfer;

import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.util.CFUtils;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

public interface DelayedExecution {

    default CompletableFuture<BatchResult> withDelayOf(Supplier<CompletableFuture<BatchResult>> result,
                                                       long delayInMillis,
                                                       ScheduledExecutorService executor) {
        try {
            return CFUtils.runFutureAfter(result, executor, delayInMillis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException("Time out occurred");
        });
    }
}
