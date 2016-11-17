package org.corfudb.util.retry;

/**
 * Created by mwei on 9/1/15.
 */
@FunctionalInterface
public interface IStrictRetryable<E extends Exception, F extends Exception, G extends Exception, H extends Exception, T>
        extends IRetryable<E, F, G, H, T> {
    /**
     * Returns true on success, false will cause retry
     */
    @Override
    T retryFunction() throws E, F, G, H;
}
