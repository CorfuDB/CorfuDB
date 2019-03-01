package org.corfudb.runtime.exceptions;

/**
 * An exception that is thrown by the logunit when it detects data corruption.
 */
public class DataCorruptionException extends LogUnitException {

    public DataCorruptionException() {
    }

    public DataCorruptionException(String message) {
        super(message);
    }

    public DataCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataCorruptionException(Throwable cause) {
        super(cause);
    }

    public DataCorruptionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
