/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

/**
 * Factory for custom futures used by the quorum replication.
 * Created by Konstantin Spirov on 2/3/2017.
 */
@Slf4j
class QuorumFutureFactory {

    /**
     * Get a future that will complete only when n/2+1 futures complete or n/2+1 futures are canceled.
     *
     * The future returned does not block explicitly, it aggregates the futures and delegates the blocking.
     *
     * In case of normal execution, any of the compete futures can be used to return the result.
     * In case of termination, the cancel flag will be updated and if any of the futures threw an exception,
     * ExecutionException will be thrown,  otherwise the future will return null
     *
     * @param futures The N futures
     * @return The composite future
     */
    static <R> Future<R> getQuorumFuture(CompletableFuture<R>... futures) {
        return getCompositeFuture(futures.length/2+1, futures);
    }

    /**
     * Get a future that will complete only when a single futures complete or n/2+1 futures are canceled.
     *
     * The future returned does not block explicitly, it aggregates the futures and delegates the blocking.
     *
     * In case if some future completes successfully its value will be returned.
     * In case of termination, the cancel flag will be updated and if any of the futures threw an exception,
     * ExecutionException will be thrown,  otherwise the future will return null
     *
     * @param futures The N futures
     * @return The composite future
     */
    static <R> Future<R> getFirstWinsFuture(CompletableFuture<R>... futures) {
        return getCompositeFuture(1, futures);
    }

    private static <R> Future<R> getCompositeFuture(int futuresToWait, CompletableFuture<R>... futures) {
        return new Future<R>() {
            private boolean done = false;
            private boolean canceled = false;

            @Override
            public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                long until = 0;
                boolean infinite = (timeout==Long.MAX_VALUE);
                if (!infinite) {
                    until = System.nanoTime() + unit.toNanos(timeout);
                }
                while (infinite || System.nanoTime() < until) {
                    Integer lastCompleteFutureIndex = null;
                    Integer lastExceptionIndex = null;
                    int numAbnormalExists = 0;
                    int numCompleteFutures = 0;
                    CompletableFuture aggregatedFuture = null; // block until some future completes
                    for (int i = 0; i < futures.length; i++) {
                        CompletableFuture<R> c = futures[i];
                        if (!c.isDone()) {
                            if (aggregatedFuture == null) {
                                aggregatedFuture = c;
                            } else {
                                aggregatedFuture = CompletableFuture.anyOf(aggregatedFuture, c);
                            }
                        } else {
                            if (c.isCancelled() ) {
                                numAbnormalExists++;
                            } else if (c.isCompletedExceptionally()) {
                                numAbnormalExists++;
                                lastExceptionIndex = i;
                            } else {
                                lastCompleteFutureIndex = i;
                                numCompleteFutures++;
                            }
                        }
                    }
                    int quorum = futures.length/2+1;
                    if (numCompleteFutures>=futuresToWait) { // normal exit, quorum
                        done = true;
                        return futures[lastCompleteFutureIndex].get();
                    }
                    if (numAbnormalExists>=quorum) { // abnormal exit, canceled or failed futures
                        done = canceled = true;
                        if (lastExceptionIndex != null) {
                            futures[lastExceptionIndex].get(); // this will throw the ExecutionException
                        }
                        return null;
                    }
                    if (infinite) {
                        aggregatedFuture.get();
                    } else {
                        aggregatedFuture.get(timeout, unit);
                    }
                }
                throw new TimeoutException();
            }

            @Override
            public R get() throws InterruptedException, ExecutionException {
                try {
                    return get(Long.MAX_VALUE, null);
                } catch (TimeoutException e) {
                    log.error(e.getMessage(), e); // not likely to happen in near future
                    return null;
                }
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                for (CompletableFuture f : futures) {
                    f.cancel(mayInterruptIfRunning);
                }
                done = canceled = true;
                return canceled;
            }

            @Override
            public boolean isCancelled() {
                return canceled;
            }

            @Override
            public boolean isDone() {
                return done;
            }
        };
    }
}
