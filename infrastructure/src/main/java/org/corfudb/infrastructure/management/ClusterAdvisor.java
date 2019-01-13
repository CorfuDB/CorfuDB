package org.corfudb.infrastructure.management;

import org.corfudb.infrastructure.management.ClusterGraph.NodeRank;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.runtime.view.Layout;

import java.util.List;
import java.util.Optional;

/**
 * {@link ClusterAdvisor} provides methods to decide the status of Corfu servers
 * (failed or healed) in a given {@link Layout} and for a specific view of the cluster
 * captured in a {@link ClusterState}. Decisions are dependant on the concrete underlying algorithm
 * corresponding to a {@link ClusterType}.
 *
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
     * Provide a list of servers in the Corfu cluster which according to the underlying algorithm
     * for {@link ClusterType} have failed. The decision is made based on the
     * given view of the cluster captured in {@link ClusterState} along with the expected
     * {@link Layout}.
     *
     * @param clusterState view of the Corfu server cluster from a client node's perspective.
     * @param layout expected layout of the cluster.
     * @param localEndpoint local node endpoint.
     * @return a node considered to have been failed according to the underlying {@link ClusterType}.
     */
    Optional<NodeRank> failedServer(ClusterState clusterState, Layout layout, String localEndpoint);

    /**
     * Provide a list of servers in the Corfu cluster which according to the underlying algorithm
     * for {@link ClusterType} have healed. The decision is made based on the
     * given view of the cluster captured in {@link ClusterState} along with the expected
     * {@link Layout}.
     *
     * @param clusterStatus view of the Corfu server cluster from a client node's perspective.
     * @param layout expected layout of the cluster.
     * @return a {@link List} of servers considered to have been healed according to the underlying
     * {@link ClusterType}.
     */
    List<String> healedServers(final ClusterState clusterStatus, final Layout layout);
}
