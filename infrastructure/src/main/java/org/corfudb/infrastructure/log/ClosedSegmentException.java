package org.corfudb.infrastructure.log;

/**
 * Created by WenbinZhu on 8/16/19.
 */
public class ClosedSegmentException extends RuntimeException {

    public ClosedSegmentException(String message) {
        super(message);
    }

    public ClosedSegmentException(Throwable cause) {
        super(cause);
    }

    public ClosedSegmentException(String message, Throwable cause) {
        super(message, cause);
    }
}
