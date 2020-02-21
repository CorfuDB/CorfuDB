package org.corfudb.logreplication.send;

public class LogEntrySyncTimeoutException extends RuntimeException {

    public LogEntrySyncTimeoutException(String message) {
        super(message);
    }
}

