package org.corfudb.infrastructure.log.statetransfer.exceptions;

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
