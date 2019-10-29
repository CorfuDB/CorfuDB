package org.corfudb.infrastructure.log.statetransfer;

import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.util.CFUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

/**
 * An interface that allows to execute a future of batch result with some delay.
 */
public interface DelayedExecution {

    /**
     * Execute a future stored in the supplier of a result with a certain delay.
     * @param future A supplier that stores a completable future that returns a batch result.
     * @param delayInMillis A delay in milliseconds to execute the result.
     * @param executor An instance of a scheduler.
     * @return A completable future of a batch result produced as a result of a delayed
     * execution of a future.
     */
    default CompletableFuture<BatchResult> withDelayOf(Supplier<CompletableFuture<BatchResult>> future,
                                                       long delayInMillis,
                                                       ScheduledExecutorService executor) {
        return CFUtils.runFutureAfter(future, executor, delayInMillis);
    }
}
