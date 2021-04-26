package org.corfudb.infrastructure.compaction.exceptions;

public class CompactionException extends RuntimeException {
    public CompactionException() {
    }

    public CompactionException(Throwable cause) {
        super(cause);
    }

    public CompactionException(String message) {
        super(message);
    }

    public CompactionException(String message, Throwable cause) {
        super(message, cause);
    }
}
