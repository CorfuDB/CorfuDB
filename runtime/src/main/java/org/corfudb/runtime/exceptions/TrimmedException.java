package org.corfudb.runtime.exceptions;

/**
 * This exception is thrown when a client tries to read an address
 * that has been trimmed.
 */
public class TrimmedException extends LogUnitException {
    public TrimmedException() {

    }

    public TrimmedException(String message) {
        super(message);
    }
}
