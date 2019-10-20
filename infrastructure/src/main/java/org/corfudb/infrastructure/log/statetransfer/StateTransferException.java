package org.corfudb.infrastructure.log.statetransfer;

/**
 * A general state transfer exception. 
 */
public class StateTransferException extends RuntimeException {
    public StateTransferException(){}

    public StateTransferException(String message) {
        super(message);
    }

    public StateTransferException(String message, Throwable cause) {
        super(message, cause);
    }

    public StateTransferException(Throwable cause) {
        super(cause);
    }
}
