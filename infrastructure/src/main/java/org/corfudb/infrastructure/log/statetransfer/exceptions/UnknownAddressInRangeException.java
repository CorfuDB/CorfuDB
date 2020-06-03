package org.corfudb.infrastructure.log.statetransfer.exceptions;

/**
 * Exception that occurs during the retrieval of the unknown addresses in
 * the range on the current log unit server.
 */
public class UnknownAddressInRangeException extends RuntimeException {

    public UnknownAddressInRangeException(String message) {
        super(message);
    }

    public UnknownAddressInRangeException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnknownAddressInRangeException(Throwable cause) {
        super(cause);
    }
}
