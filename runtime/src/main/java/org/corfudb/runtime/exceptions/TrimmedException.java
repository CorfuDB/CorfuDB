package org.corfudb.runtime.exceptions;

import lombok.Getter;
import lombok.Setter;

/**
 * This exception is thrown when a client tries to read an address
 * that has been trimmed.
 */
public class TrimmedException extends LogUnitException {

    /*
     * This flag determins whether the operation that caused this exception
     * can retry or not.
     */
    @Getter
    @Setter
    private boolean retriable = true;

    public TrimmedException() {
    }

    public TrimmedException(String message) {
        super(message);
        retriable = true;
    }
}
