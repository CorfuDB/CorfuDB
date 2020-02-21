package org.corfudb.logreplication.send;

public class IllegalTransactionStreamsException extends RuntimeException {

    public IllegalTransactionStreamsException (String message) {
        super(message);
    }
}
