package org.corfudb.infrastructure.management;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.management.failuredetector.ClusterGraph;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;

import java.util.List;
import java.util.Optional;

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
     * @param clusterState        represents the state of connectivity amongst the Corfu cluster
     *                            nodes from a node's perspective.
     * @param unresponsiveServers list of unresponsive servers.
     * @return a server considered as failed according to the underlying strategy.
     */
    @Override
    public Optional<NodeRank> failedServer(
            ClusterState clusterState, List<String> unresponsiveServers, String localEndpoint) {

        log.trace("Detecting failed nodes for: ClusterState= {} unresponsive servers= {}",
                clusterState, unresponsiveServers
        );

        ClusterGraph graph = ClusterGraph.transform(clusterState);

        ClusterGraph symmetric = graph.toSymmetric();
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

        if (unresponsiveServers.contains(failedNode.getEndpoint())) {
            return Optional.empty();
        }

        log.debug("Failed node found: {}", failedNode);
        return Optional.of(failedNode);
    }

    /**
     * Provide a server considered to have healed in the Corfu cluster according to
     * the COMPLETE_GRAPH implementation of algorithm for
     * {@link ClusterType}.
     * <p>
     * The node can heal only itself. The node responsible only for itself, can't heal other nodes.
     * It simplifies healing algorithm and guaranties that if node became available it mark itself as a responsible
     * node in the layout. It helps us to simplify analysis/debugging process and brings simple and reliable algorithm
     * for healing process.
     *
     * @param clusterState        represents the state of connectivity amongst the Corfu cluster
     *                            nodes from a node's perspective.
     * @param unresponsiveServers unresponsive servers in a layout.
     * @return a {@link List} of servers considered as healed according to the underlying
     * {@link ClusterType}.
     */
    @Override
    public Optional<NodeRank> healedServer(
            ClusterState clusterState, List<String> unresponsiveServers, String localEndpoint) {

        log.trace("Detecting the healed nodes for: ClusterState: {} unresponsive servers: {}",
                clusterState, unresponsiveServers
        );

        if (unresponsiveServers.isEmpty()) {
            log.trace("All nodes responsive. Nothing to heal");
            return Optional.empty();
        }

        if (!unresponsiveServers.contains(localEndpoint)){
            log.trace("Local node is responsive. Nothing to heal");
            return Optional.empty();
        }

        ClusterGraph symmetricGraph = ClusterGraph.transform(clusterState).toSymmetric();
        return symmetricGraph.findFullyConnectedResponsiveNode(
                localEndpoint, unresponsiveServers
        );
    }
}
