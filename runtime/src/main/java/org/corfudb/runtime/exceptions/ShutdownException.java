package org.corfudb.runtime.exceptions;

/**
 * Created by zlokhandwala on 11/22/17.
 */
public class ShutdownException extends RuntimeException {

    public ShutdownException() {
    }

    public ShutdownException(String message) {
        super(message);
    }
}
