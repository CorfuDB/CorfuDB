package org.corfudb.infrastructure.logreplication.replication.send;

public class IllegalSnapshotEntrySizeException extends RuntimeException {
    public IllegalSnapshotEntrySizeException(String message) {
        super(message);
    }
}
