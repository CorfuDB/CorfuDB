package org.corfudb.runtime.exceptions;

/**
 * Created by mwei on 4/7/17.
 */
public class RecoveryException extends RuntimeException {
    public RecoveryException(String message) {
        super(message);
    }
}
