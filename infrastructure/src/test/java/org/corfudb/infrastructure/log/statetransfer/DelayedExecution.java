package org.corfudb.infrastructure.log.statetransfer;

import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.util.CFUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

public interface DelayedExecution {

    default CompletableFuture<BatchResult> withDelayOf(Supplier<CompletableFuture<BatchResult>> result,
                                                       long delayInMillis,
                                                       ScheduledExecutorService executor) {
        return CFUtils.runFutureAfter(result, executor, delayInMillis);
    }
}
