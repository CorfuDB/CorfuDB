package org.corfudb.runtime.exceptions;

/**
 * An exception that is thrown by the logunit when it detects data corruption.
 */
public class DataCorruptionException extends LogUnitException {

    public DataCorruptionException() {
        super();
    }

    public DataCorruptionException(long currentAddress, long committedAddress) {
        super(String.format("detected data corruption at address %d/%d.",
                currentAddress, committedAddress));
    }
}
