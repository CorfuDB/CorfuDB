package org.corfudb.universe.cluster;

/**
 * This class represents common {@link Cluster} exception wrapping the problems that prevented a successful operation
 * on {@link Cluster}
 */
public class ClusterException extends RuntimeException {
    public ClusterException() {
        super();
    }

    public ClusterException(String message) {
        super(message);
    }

    public ClusterException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClusterException(Throwable cause) {
        super(cause);
    }

    protected ClusterException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
