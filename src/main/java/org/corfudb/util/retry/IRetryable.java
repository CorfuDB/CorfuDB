package org.corfudb.util.retry;

/**
 * Created by mwei on 9/1/15.
 */
@FunctionalInterface
public interface IRetryable {
    /** Returns true on success, false will cause retry */
    boolean retryFunction() throws Exception;
}
