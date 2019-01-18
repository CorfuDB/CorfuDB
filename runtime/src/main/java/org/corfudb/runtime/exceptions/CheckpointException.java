package org.corfudb.runtime.exceptions;

/**
 * Exception thrown when a checkpointer thread fails.
 */
public class CheckpointException extends RuntimeException {
    public CheckpointException(Throwable t) {
        super((t));
    }

    public CheckpointException(String msg) {
        super(msg);
    }
}