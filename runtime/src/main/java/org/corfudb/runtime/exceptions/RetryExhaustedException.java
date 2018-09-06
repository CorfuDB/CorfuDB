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
}
