package org.corfudb.runtime.exceptions;

/**
 * RetryExhaustedException is thrown when there is no hope in retrying and the current progress
 * needs to be aborted.
 * Created by zlokhandwala on 8/29/18.
 */
public class RetryExhaustedException extends RuntimeException {

    public RetryExhaustedException(String s) {
        super(s);
    }

    public RetryExhaustedException() {
    }

    public RetryExhaustedException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryExhaustedException(Throwable cause) {
        super(cause);
    }

    public RetryExhaustedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
