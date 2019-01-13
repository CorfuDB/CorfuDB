package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.management.ClusterGraph.NodeRank;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.runtime.view.Layout;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * This class is an implementation of {@link ClusterType} where the ideal state
 * of the Corfu cluster is a fully connected network (i.e. complete graph) in which there is an
 * active link amongst all nodes in the cluster. Failed and Healed nodes are recommended so that
 * the cluster remains fully connected.
 * <p>
 * Created by Sam Behnam on 10/27/18.
 */
@Slf4j
public class CompleteGraphAdvisor implements ClusterAdvisor {

    private static final ClusterType CLUSTER_TYPE = ClusterType.COMPLETE_GRAPH;

    @Override
    public ClusterType getType() {
        return CLUSTER_TYPE;
    }

    /**
     * Provides list of servers from a given layout(epoch) that this implementation of
     * COMPLETE_GRAPH algorithm has determined as failed. The implementation of the
     * algorithm in this method is an approach by executing the following steps:
     * <p>
     * The failed node is the recommendation of this strategy which their removal
     * from cluster will lead to a fully connected cluster.
     * <p>
     * The following represents the underlying implementation of one algorithm to achieve the
     * above goal however the clients of this strategy must only rely on the guarantee that removal
     * of the returned failed node is a recommendation for arriving at a fully connected
     * cluster and must not rely on the implementation details of the algorithm.
     * <p>
     *
     * @param clusterState represents the state of connectivity amongst the Corfu cluster
     *                     nodes from a node's perspective.
     * @param layout       expected layout of the cluster.
     * @return a server considered as failed according to the underlying strategy.
     */
    @Override
    public Optional<NodeRank> failedServer(ClusterState clusterState, Layout layout, String localEndpoint) {
        log.trace("Detecting the failed nodes for: ClusterState= {} Layout= {}", clusterState, layout);

        if (clusterState.size() != layout.getAllServers().size()) {
            log.error("Cluster representation is different than layout. Cluster: {}, layout: {}", clusterState, layout);
            return Optional.empty();
        }

        ClusterGraph symmetric = ClusterGraph.transform(clusterState).toSymmetric();
        Optional<NodeRank> maybeDecisionMaker = symmetric.getDecisionMaker();

        if (!maybeDecisionMaker.isPresent()) {
            return Optional.empty();
        }

        NodeRank decisionMaker = maybeDecisionMaker.get();
        if (!decisionMaker.is(localEndpoint)) {
            String message = "The node can't be a decision maker, skip operation. Decision maker node is: {}";
            log.debug(message, decisionMaker);
            return Optional.empty();
        }

        Optional<NodeRank> maybeFailedNode = symmetric.findFailedNode();
        if (!maybeFailedNode.isPresent()) {
            return Optional.empty();
        }

        NodeRank failedNode = maybeFailedNode.get();

        if (decisionMaker.equals(failedNode)) {
            log.error("Decision maker and failed node are same node: {}", decisionMaker);
            return Optional.empty();
        }

        if (layout.getUnresponsiveServers().contains(failedNode.getEndpoint())) {
            return Optional.empty();
        }

        return Optional.of(failedNode);
    }

    /**
     * Provide a list of servers considered to have healed in the Corfu cluster according to
     * the COMPLETE_GRAPH implementation of algorithm for
     * {@link ClusterType}. The implementation of the algorithm in this method
     * is a greedy implementation through the execution of the following steps:
     * <p>
     * a) Add all unresponsive nodes with active links to the entire set of responsive nodes in the
     * cluster will be collected
     * b) Greedily add the nodes with minimum number of failed links which are are fully
     * connected to the set of active nodes as well as to the proposed set of healed nodes
     * <p>
     * Output of the above steps recommends the healed nodes which their addition will increase
     * the number of active members of the Corfu cluster while keeping it as a fully connected
     * cluster.
     * <p>
     * The following represents the underlying implementation of one algorithm to achieve the
     * above goal however the clients of this strategy must only rely on the guarantee that addition
     * of returned healed nodes is a recommendation for increasing responsive servers in
     * cluster while keeping the responsive cluster as a fully connected corfu cluster. The
     * clients must not rely on the implementation details of the algorithm as it might change in
     * the future releases.
     * <p>
     * Find Healed Nodes algorithm:
     * <p>
     * // Create the super set of healed nodes
     * for (Node in Unresponsive Nodes Set):
     * reEstablishedLinksMap.put(Node, set of peers that sent successful heartbeat to
     * Node)
     * <p>
     * // Collect super set of healed nodes
     * for (entry in reEstablishedLinksMap):
     * if (Responsive Nodes Set is NOT subset of Nodes Set of entry):
     * reEstablishedLinksMap.remove(Node of entry)
     * <p>
     * // Descending sort of the nodes based on number of reestablished links
     * sortedReEstablishedLinksMap <- descending sort by size of Node Set of
     * reEstablishedLinksMap
     * <p>
     * // Greedily add fully connected nodes with highest number of established links while
     * // keeping the invariant of Complete Graph
     * Proposed Healed Set = {}
     * <p>
     * for (entry in sortedReEstablishedLinksMap):
     * if ((Responsive Node Set union with Proposed Healed Set) is subset of Node Set of
     * entry):
     * add Node of entry to Proposed Healed Set
     * <p>
     * return Proposed Healed Set
     *
     * @param clusterState represents the state of connectivity amongst the Corfu cluster
     *                     nodes from a node's perspective.
     * @param layout       expected layout of the cluster.
     * @return a {@link List} of servers considered as healed according to the underlying
     * {@link ClusterType}.
     */
    @Override
    public Optional<NodeRank> healedServer(ClusterState clusterState, Layout layout, String localEndpoint) {
        log.trace("Detecting the healed nodes for: ClusterState: {} Layout: {}", clusterState, layout);

        if (layout.getUnresponsiveServers().isEmpty()) {
            return Optional.empty();
        }

        ClusterGraph symmetricGraph = ClusterGraph.transform(clusterState).toSymmetric();
        ImmutableMap<String, NodeRank> responsiveNodes = symmetricGraph.findFullyConnectedResponsiveNodes(
                layout.getUnresponsiveServers()
        );

        return Optional.ofNullable(responsiveNodes.get(localEndpoint));
    }
}
