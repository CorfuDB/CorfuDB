package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.management.failuredetector.ClusterGraph;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;

import java.util.List;
import java.util.Optional;
import java.util.Set;

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
     * To find a failed node:
     * - get decision maker
     * - find a failed node. Check if decision maker is not equal to failed node and failed node is not in unresponsive
     * servers already.
     *
     * @param clusterState represents the state of connectivity amongst the Corfu cluster
     *                     nodes from a node's perspective.
     * @return a server considered as failed according to the underlying strategy.
     */
    @Override
    public Optional<NodeRank> failedServer(ClusterState clusterState) {

        log.trace("Detecting failed nodes for: ClusterState= {}", clusterState);

        ClusterGraph symmetric = ClusterGraph
                .toClusterGraph(clusterState)
                .toSymmetric();

        Optional<NodeRank> maybeFailedNode = symmetric.findFailedNode();

        if (!maybeFailedNode.isPresent()) {
            return Optional.empty();
        }

        NodeRank failedNode = maybeFailedNode.get();

        ImmutableList<String> unresponsiveNodes = clusterState.getUnresponsiveNodes();
        if (unresponsiveNodes.contains(failedNode.getEndpoint())) {
            log.trace("Failed node already in the list of unresponsive nodes: {}", unresponsiveNodes);
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
     * @param clusterState represents the state of connectivity amongst the Corfu cluster
     *                     nodes from a node's perspective.
     * @return a {@link List} of servers considered as healed according to the underlying
     * {@link ClusterType}.
     */
    @Override
    public Optional<NodeRank> healedServer(ClusterState clusterState, String localEndpoint) {

        log.trace("Detecting the healed nodes for: ClusterState: {}", clusterState);

        ImmutableList<String> unresponsiveNodes = clusterState.getUnresponsiveNodes();
        if (unresponsiveNodes.isEmpty()) {
            log.trace("All nodes responsive. Nothing to heal");
            return Optional.empty();
        }

        if (!unresponsiveNodes.contains(localEndpoint)) {
            log.trace("Local node is responsive. Nothing to heal");
            return Optional.empty();
        }

        //Transform a ClusterState to the ClusterGraph and make it symmetric (symmetric failures)
        ClusterGraph symmetricGraph = ClusterGraph.toClusterGraph(clusterState).toSymmetric();

        //See if local node is healed.
        return symmetricGraph.findFullyConnectedNode(localEndpoint);
    }

    /**
     * Returns a new cluster graph from the cluster state
     *
     * @param clusterState a cluster state
     * @return a transformed cluster graph
     */
    @Override
    public ClusterGraph getGraph(ClusterState clusterState) {
        return ClusterGraph.toClusterGraph(clusterState).toSymmetric();
    }

    @Override
    public Optional<NodeRank> findDecisionMaker(ClusterState clusterState, Set<String> healthyNodes) {
        ClusterGraph symmetric = ClusterGraph
                .toClusterGraph(clusterState)
                .toSymmetric();

        return symmetric.getDecisionMaker(healthyNodes);
    }
}
