package org.corfudb.infrastructure.logreplication.replication.send;

public class IllegalLogDataSizeException extends RuntimeException {
    public IllegalLogDataSizeException(String message) {
        super(message);
    }
}
