package org.corfudb.runtime.view;

import java.util.Map;

import lombok.Data;
import lombok.Getter;

/**
 * Status report of the connectivity of the client to the cluster and the health of the cluster
 * based on the views of the management agents on the Corfu nodes.
 *
 * <p>Created by zlokhandwala on 5/7/18.
 */
@Data
public class ClusterStatusReport {

    /**
     * Connectivity to the node.
     */
    public enum NodeStatus {

        /**
         * Node is reachable.
         */
        UP,

        /**
         * Node is reachable but does not have complete data redundancy.
         */
        DB_SYNCING,

        /**
         * Node is not reachable.
         */
        DOWN
    }

    /**
     * Health of the cluster.
     */
    public enum ClusterStatus {

        /**
         * The cluster is stable and all nodes are operational.
         */
        STABLE(0),

        /**
         * The cluster is operational but working with reduced redundancy.
         */
        DEGRADED(1),

        /**
         * The cluster is not operational.
         */
        UNAVAILABLE(2);

        @Getter
        final int healthValue;

        ClusterStatus(int healthValue) {
            this.healthValue = healthValue;
        }
    }

    /**
     * Layout at which the report was generated.
     */
    private final Layout layout;

    /**
     * Map of connectivity of the report generator client to the cluster nodes.
     */
    private final Map<String, NodeStatus> clientServerConnectivityStatusMap;

    /**
     * Cluster health.
     */
    private final ClusterStatus clusterStatus;
}
