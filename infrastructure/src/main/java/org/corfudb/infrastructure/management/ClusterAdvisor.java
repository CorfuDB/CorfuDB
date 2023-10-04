package org.corfudb.infrastructure.management;

import org.corfudb.infrastructure.management.failuredetector.ClusterGraph;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.runtime.view.Layout;

import java.util.Optional;
import java.util.Set;

/**
 * {@link ClusterAdvisor} provides methods to decide the status of Corfu servers
 * (failed or healed) in a given {@link Layout} and for a specific view of the cluster
 * captured in a {@link ClusterState}. Decisions are dependent on the concrete underlying algorithm
 * corresponding to a {@link ClusterType}.
 * <p>
 * Created by Sam Behnam on 10/19/18.
 */
public interface ClusterAdvisor {

    /**
     * Get the corresponding {@link ClusterType} used in the current instance of
     * {@link ClusterAdvisor}. This strategy represents the characteristics of the
     * underlying algorithm used for making a decision about the failed or healed status of
     * Corfu servers.
     *
     * @return a concrete instance of {@link ClusterType}
     */
    ClusterType getType();

    /**
     * Provide a server in the Corfu cluster which according to the underlying algorithm
     * for {@link ClusterType} have failed. The decision has been made based on the
     * given view of the cluster status captured in {@link ClusterState} along with the expected
     * {@link Layout}.
     *
     * @param clusterState view of the Corfu server cluster from a client node's perspective.
     * @return a node considered to have been failed according to the underlying {@link ClusterType}.
     */
    Optional<NodeRank> failedServer(ClusterState clusterState);

    /**
     * Provide a server in the Corfu cluster which according to the underlying algorithm
     * for {@link ClusterType} have healed. The decision is made based on the
     * given view of the cluster captured in {@link ClusterState} along with the expected
     * {@link Layout}.
     *
     * @param clusterState view of the Corfu server cluster from a client node's perspective.
     * @return a server considered to have been healed according to the underlying
     * {@link ClusterType}.
     */
    Optional<NodeRank> healedServer(ClusterState clusterState, String localEndpoint);

    /**
     * Provides a cluster graph generated from the {@link ClusterState}
     *
     * @return ClusterGraph
     */
    ClusterGraph getGraph(ClusterState clusterState);

    Optional<NodeRank> findDecisionMaker(ClusterState clusterState, Set<String> healthyNodes);
}
