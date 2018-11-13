package org.corfudb.infrastructure.management;

import org.corfudb.protocols.wireprotocol.NodeView;
import org.corfudb.runtime.view.Layout;

import java.util.List;

/**
 * {@link ClusterRecommendationEngine} provides methods to decide the status of Corfu servers
 * (failed or healed) in a given {@link Layout} and for a specific view of the cluster
 * captured in a {@link NodeView}. Decisions are dependant on the concrete underlying algorithm
 * corresponding to a {@link ClusterRecommendationStrategy}.
 *
 * Created by Sam Behnam on 10/19/18.
 */
public interface ClusterRecommendationEngine {

    /**
     * Get the corresponding {@link ClusterRecommendationStrategy} used in the current instance of
     * {@link ClusterRecommendationEngine}. This strategy represents the characteristics of the
     * underlying algorithm used for making a decision about the failed or healed status of
     * Corfu servers.
     *
     * @return a concrete instance of {@link ClusterRecommendationStrategy}
     */
    ClusterRecommendationStrategy getClusterRecommendationStrategy();

    /**
     * Provide a list of servers in the Corfu cluster which according to the underlying algorithm
     * for {@link ClusterRecommendationStrategy} have failed. The decision is made based on the
     * given view of the cluster captured in {@link NodeView} along with the expected
     * {@link Layout}.
     *
     * @param nodeView view of the Corfu cluster of servers from a client node's perspective.
     * @param layout expected layout of the cluster.
     * @return a {@link List} of Corfu servers suspected to have been failed according to the
     * underlying {@link ClusterRecommendationStrategy}.
     */
    List<String> suspectedFailedServers(NodeView nodeView, Layout layout);

    /**
     * Provide a list of servers in the Corfu cluster which according to the underlying algorithm
     * for {@link ClusterRecommendationStrategy} have healed. The decision is made based on the
     * given view of the cluster captured in {@link NodeView} along with the expected
     * {@link Layout}.
     *
     * @param nodeView view of the Corfu cluster of servers from a client node's perspective.
     * @param layout expected layout of the cluster.
     * @return a {@link List} of servers suspected to have been healed according to the underlying
     * {@link ClusterRecommendationStrategy}.
     */
    List<String> suspectedHealedServers(NodeView nodeView, Layout layout);
}
