package org.corfudb.runtime.exceptions;

/**
 * An exception that is thrown when a write tries to write more than
 * the max write limit.
 */
public class WriteSizeException extends RuntimeException {
    public WriteSizeException(int size, int max) {
        super("Trying to write " + size + " bytes but max write limit is " + max + " bytes");
    }
}
