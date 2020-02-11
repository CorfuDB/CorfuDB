package org.corfudb.logreplication.transmit;

public class ReplicationReaderException extends RuntimeException {
    public ReplicationReaderException() { }

    public ReplicationReaderException(String message) {
        super(message);
    }

    public ReplicationReaderException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReplicationReaderException(Throwable cause) {
        super(cause);
    }
}
