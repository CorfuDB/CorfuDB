package org.corfudb.universe.node;

/**
 * This class represents common {@link Node} exception wrapping the
 * problems that prevented a successful operation on a {@link Node}
 */
public class NodeException extends RuntimeException {

    public NodeException() {
    }

    public NodeException(String message) {
        super(message);
    }

    public NodeException(String message, Throwable cause) {
        super(message, cause);
    }

    public NodeException(Throwable cause) {
        super(cause);
    }

    public NodeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
