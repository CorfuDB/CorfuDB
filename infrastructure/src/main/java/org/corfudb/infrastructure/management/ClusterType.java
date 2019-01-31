package org.corfudb.infrastructure.management;

/**
 * {@link ClusterType} is the enumeration of policies each of which representing
 * specific algorithms for evaluating the cluster and providing recommendation on the failure or
 * healing status of nodes in a Corfu server cluster.
 *
 * Created by Sam Behnam on 10/19/18.
 */
public enum ClusterType {
    /**
     * COMPLETE_GRAPH represents a cluster evaluation approach for recommending the
     * failed and healed status of node in which the implementing algorithm:
     * Determines a node to have failed when the given node is NOT FULLY connected to all the
     * healthy servers in the cluster.
     * Determines a node to have healed when the given node is FULLY connected to all the healthy
     * servers in the cluster.
     *
     * After applying this strategy, the resulting graph of cluster nodes will resemble a Complete
     * Graph.
     */
    COMPLETE_GRAPH,
    /**
     * STAR_GRAPH represents a cluster evaluation approach for recommending the
     * failed and healed status of node in which the implementing algorithm:
     * Determines a node to have failed when the given node is NOT connected to at least one
     * CENTRAL node which in turn is connected to all the healthy servers in the cluster.
     * Determines a node to have healed when the given node is connected to at least one
     * CENTRAL node which in turn is connected to all the healthy servers in the cluster.
     *
     * After applying this strategy, the resulting graph of cluster nodes will resemble a Star
     * Topology Graph.
     */
    STAR_GRAPH
}
