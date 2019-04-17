package org.corfudb.runtime.exceptions;

/**
 * Created by mwei on 12/14/15.
 */
public class LogUnitException extends RuntimeException {

    public LogUnitException() {
    }

    public LogUnitException(String message) {
        super(message);
    }

    public LogUnitException(String message, Throwable cause) {
        super(message, cause);
    }

    public LogUnitException(Throwable cause) {
        super(cause);
    }

    public LogUnitException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
