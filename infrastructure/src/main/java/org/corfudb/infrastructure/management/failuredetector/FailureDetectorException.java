package org.corfudb.infrastructure.management.failuredetector;

import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.runtime.view.Layout;

public class FailureDetectorException extends RuntimeException {


    public FailureDetectorException() {
    }

    public FailureDetectorException(String message) {
        super(message);
    }

    public FailureDetectorException(String message, Throwable cause) {
        super(message, cause);
    }

    public FailureDetectorException(Throwable cause) {
        super(cause);
    }

    public FailureDetectorException(
            String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public static FailureDetectorException layoutMismatch(ClusterState clusterState, Layout layout) {
        String errStr = "Cluster representation is different than layout. Cluster: %s, layout: %s";
        String err = String.format(errStr, clusterState, layout);
        return new FailureDetectorException(err);
    }

    public static FailureDetectorException notReady(ClusterState clusterState) {
        String err = String.format("Cluster state is not ready: %s", clusterState);
        return new FailureDetectorException(err);
    }

    public static FailureDetectorException disconnected() {
        String err = "Error in correcting server epochs. Local node is disconnected";
        return new FailureDetectorException(err);
    }
}
