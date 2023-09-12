package org.corfudb.runtime.exceptions;

public class SubscribeMaxRetryException extends RuntimeException {
    public SubscribeMaxRetryException() {
        super("Max number of subscription retries reached.");
    }
}
