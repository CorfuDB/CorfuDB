package org.corfudb.util.retry;

/**
 * Created by mwei on 9/1/15.
 */
@FunctionalInterface
public interface IRetryable<E extends Exception, F extends Exception,
        G extends Exception, H extends Exception, T> {
    /**
     * Returns true on success, false will cause retry.
     */
    T retryFunction() throws  RetryNeededException, InterruptedException, E, F, G, H;

}
