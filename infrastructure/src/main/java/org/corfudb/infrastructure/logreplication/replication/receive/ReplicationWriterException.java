package org.corfudb.infrastructure.logreplication.replication.receive;

public class ReplicationWriterException extends RuntimeException {
    public ReplicationWriterException() { }

    public ReplicationWriterException(String message) {
        super(message);
    }

    public ReplicationWriterException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReplicationWriterException(Throwable cause) {
        super(cause);
    }
}
