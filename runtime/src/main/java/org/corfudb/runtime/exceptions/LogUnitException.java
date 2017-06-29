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
}
