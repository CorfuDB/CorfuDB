package org.corfudb.infrastructure.logreplication.replication.send;

public class LogEntrySyncTimeoutException extends RuntimeException {

    public LogEntrySyncTimeoutException(String message) {
        super(message);
    }
}

