package org.corfudb.infrastructure.management.failuredetector;

import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.runtime.view.Layout;

import java.util.List;
import java.util.Map;

/**
 * A common exception for all types of Failure Detection exceptional situations
 */
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

    public static FailureDetectorException disconnected(List<String> layoutServers, Map<String, Long> wrongEpochs) {
        String err = String.format(
                "Can't get a layout from any server in the cluster. Local node is disconnected. " +
                        "Layout servers: %s, wrong epochs: %s",
                layoutServers, wrongEpochs
        );
        return new FailureDetectorException(err);
    }

    public static FailureDetectorException notInCluster(String localEndpoint, Layout layout) {
        String err = String.format(
                "Can't run failure detector. This Server: %s, doesn't belong to the active layout: %s",
                localEndpoint, layout
        );

        return new FailureDetectorException(err);
    }
}
