package org.corfudb.infrastructure.logreplication.replication.send;

public class IllegalTransactionStreamsException extends RuntimeException {

    public IllegalTransactionStreamsException (String message) {
        super(message);
    }
}
