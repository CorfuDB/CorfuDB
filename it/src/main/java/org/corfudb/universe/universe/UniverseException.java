package org.corfudb.universe.universe;

/**
 * This class represents common {@link Universe} exception wrapping the problems that prevented a successful operation
 * on {@link Universe}
 */
public class UniverseException extends RuntimeException {
    public UniverseException() {
        super();
    }

    public UniverseException(String message) {
        super(message);
    }

    public UniverseException(String message, Throwable cause) {
        super(message, cause);
    }

    public UniverseException(Throwable cause) {
        super(cause);
    }

    protected UniverseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
