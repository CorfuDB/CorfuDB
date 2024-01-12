package org.corfudb.infrastructure.management.failuredetector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.failuredetector.FailureDetectorMetrics.ConnectivityGraph;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.NodeConnectivityType;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.util.JsonUtils;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Graph representation of a cluster state. For instance, asymmetric graph,
 * there is an asymmetric failure between nodes `a` and `b`. {@link ConnectionStatus} could be OK/FAILED:
 * {a -> {{a: OK}, {b: FAILED}, {c: OK}}}
 * {b -> {{a: FAILED}, {b: OK}, {c: FAILED}}}
 * {c -> {{a: OK}, {b: false}, {c: OK}}}
 * <p>
 * This class represents a cluster as a graph, it allows to:
 * - find fully connected node: node that connected to all other nodes exclude unresponsive nodes.
 * - find a decision maker: a node which should decide to add failed node to the unresponsive list.
 * The decision maker is an optimization technique, needed to reduce parallel updates of the cluster state.
 * Still, each node can choose its own decision maker. See {@link this#getDecisionMaker()} for details
 * - find a node which should be healed and excluded from the unresponsive list
 * - transform all asymmetric failures to a symmetric ones. If there is an asymmetric failure
 * (node B can connect to node B, but node B can not connect to node A) between two nodes
 * then make it symmetric which means neither node A nor B can connect to each other.
 */
@Builder
@ToString
@Slf4j
public class ClusterGraph {

    @NonNull
    private final ImmutableMap<String, NodeConnectivity> graph;

    @NonNull
    private final String localNode;

    @NonNull
    private final ImmutableList<String> unresponsiveNodes;

    /**
     * Transform a cluster state to the cluster graph.
     * ClusterState contains some extra information, cluster graph is a pure representation of a graph of nodes.
     * To be able to work efficiently with cluster state to find failed and healed nodes the cluster state should be
     * transformed to cluster graph
     *
     * @param cluster cluster state
     * @return cluster graph
     */
    public static ClusterGraph toClusterGraph(ClusterState cluster) {
        Map<String, NodeConnectivity> graph = cluster.getNodes()
                .values()
                .stream()
                .map(NodeState::getConnectivity)
                .collect(Collectors.toMap(NodeConnectivity::getEndpoint, Function.identity()));

        return ClusterGraph.builder()
                .localNode(cluster.getLocalEndpoint())
                .unresponsiveNodes(cluster.getUnresponsiveNodes())
                .graph(ImmutableMap.copyOf(graph))
                .build();
    }

    /**
     * Convert a cluster graph which could have asymmetric failures to a graph with symmetric failures between nodes.
     * See {@link ConnectionStatus} for OK/FAILED
     * For instance:
     * {a: [{a: OK, b: FAILED}]}
     * {b: [{a: OK, b: OK}]}
     * Node A believes that node B is disconnected
     * Node B believes that node A is connected.
     * The graph will be changed to:
     * {a: [{a: OK, b: FAILED}]}
     * {b: [{a: FAILED, b: OK}]}
     * Node B is not connected to node A anymore.
     *
     * @return a graph with symmetric failures between nodes.
     */
    public ClusterGraph toSymmetric() {
        Map<String, NodeConnectivity> symmetric = new HashMap<>();

        graph.keySet().forEach(nodeName -> {
            NodeConnectivity node = graph.get(nodeName);
            if (node.getType() == NodeConnectivityType.UNAVAILABLE) {
                symmetric.put(nodeName, node);
                return;
            }

            Map<String, ConnectionStatus> newConnectivity = new HashMap<>();
            node.getConnectivity()
                    //Get list of adjacent nodes to this node
                    .keySet()
                    // For all adjacent nodes figure out if the node is connected to current node
                    .forEach(adjNodeName -> {
                        if (!graph.containsKey(adjNodeName)) {
                            return;
                        }

                        NodeConnectivity adjNode = graph.get(adjNodeName);

                        // If current node is not the local node and another node is unavailable we don't change
                        // the adjacent node connectivity matrix, we leave it as is
                        if (adjNode.getType() == NodeConnectivityType.UNAVAILABLE && !isLocalNode(node)) {
                            newConnectivity.put(adjNodeName, node.getConnectionStatus(adjNodeName));
                            return;
                        }

                        //Get connection status for current node
                        ConnectionStatus nodeConnection = getConnectionStatus(node, adjNode);
                        //Get connection status for opposite node
                        ConnectionStatus oppositeNodeConnection = getConnectionStatus(adjNode, node);

                        //Symmetric failure - connection successful only if both nodes connected status is true
                        //in the other case - make the failure symmetric
                        ConnectionStatus status = ConnectionStatus.OK;
                        if (EnumSet.of(nodeConnection, oppositeNodeConnection).contains(ConnectionStatus.FAILED)) {
                            status = ConnectionStatus.FAILED;
                        }
                        newConnectivity.put(adjNodeName, status);
                    });

            NodeConnectivity symmetricConnectivity = NodeConnectivity.builder()
                    .endpoint(nodeName)
                    .epoch(node.getEpoch())
                    .connectivity(ImmutableMap.copyOf(newConnectivity))
                    .type(node.getType())
                    .build();

            symmetric.put(nodeName, symmetricConnectivity);
        });

        //Provide a new graph with symmetric failures
        return ClusterGraph.builder()
                .localNode(localNode)
                .unresponsiveNodes(unresponsiveNodes)
                .graph(ImmutableMap.copyOf(symmetric))
                .build();
    }

    private boolean isLocalNode(NodeConnectivity node) {
        return node.getEndpoint().equals(localNode);
    }

    /**
     * Get a decision maker node to detect a failure. It must have:
     * - highest number of successful connections in the graph.
     * There could be many nodes with same number of successful connections,
     * then the decision maker will be a node with smallest name.
     * <p>
     * The decision maker is an optimization technique, needed to reduce parallel updates of the cluster state.
     * Still, each node can choose its own decision maker.
     * Note: it's not required to choose a particular one for entire cluster to add a node to unresponsive list.
     * In a partitioned scenario, a minority side can choose to have a decision maker
     * just that it's decisions will not be used as it does not have consensus
     * <p>
     * The decision maker always exists. There is always at least the local node,
     * it always has at least one successful connection to itself.
     * We also have two additional checks to prevent all possible incorrect ways. Decision maker not found if:
     * - ClusterGraph is empty, which is an invalid state.
     * - the decision maker doesn't have connections, which is also impossible.
     *
     * @param healthyNodes the list of unhealthy nodes from other detectors/agents (like read-only file system)
     * @return a decision maker node
     */
    public Optional<NodeRank> getDecisionMaker(Set<String> healthyNodes) {
        log.trace("Get decision maker");

        NavigableSet<NodeRank> healthyDecisionMakers = getNodeRanks().stream()
                .filter(nodeRank -> healthyNodes.contains(nodeRank.getEndpoint()))
                .collect(Collectors.toCollection(TreeSet::new));

        Optional<NodeRank> maybeDecisionMaker = Optional.ofNullable(healthyDecisionMakers.pollFirst());

        if (!maybeDecisionMaker.isPresent()) {
            log.error("Decision maker not found for graph: {}", toJson());
            return Optional.empty();
        }

        return maybeDecisionMaker
                //filter out completely disconnected decision maker
                .filter(decisionMaker -> {
                    boolean isConnected = decisionMaker.getNumConnections() >= 1;
                    if (!isConnected) {
                        log.trace("The node is fully disconnected from the graph. Decision maker doesn't exists");
                    }

                    return isConnected;
                })
                //Give up if the local node is not a decision maker
                .filter(decisionMaker -> {
                    boolean isLocalNodeADecisionMaker = decisionMaker.is(localNode);
                    if (!isLocalNodeADecisionMaker) {
                        String message = "The node can't be a decision maker, skip operation. Decision maker node is: {}";
                        log.trace(message, decisionMaker);
                    }

                    return isLocalNodeADecisionMaker;
                });
    }

    /**
     * Find failed node in a graph.
     * Collect all node ranks in a graph and choose the node with smallest number of successful connections and
     * with highest name (sorted alphabetically)
     *
     * @return failed node
     */
    public Optional<NodeRank> findFailedNode() {
        log.trace("Looking for failed node");

        NavigableSet<NodeRank> nodes = getNodeRanks();
        if (nodes.isEmpty()) {
            log.error("Empty graph. Can't provide failed node");
            return Optional.empty();
        }

        NodeRank last = nodes.last();
        if (last.getNumConnections() == graph.size()) {
            return Optional.empty();
        }

        //If the node is connected to all alive nodes (nodes not in unresponsive list)
        // in the cluster, that node can't be a failed node.
        // We can't rely on information from nodes in unresponsive list.
        Optional<NodeRank> fullyConnected = findFullyConnectedNode(last.getEndpoint());

        //check if failed node is fully connected
        boolean isFailedNodeFullyConnected = fullyConnected
                .map(fcNode -> fcNode.equals(last))
                .orElse(false);

        if (isFailedNodeFullyConnected) {
            log.trace("Fully connected node can't be a failed node");
            return Optional.empty();
        }

        return Optional.of(last);
    }

    /**
     * See if the node is fully connected.
     *
     * @param endpoint local node name
     * @return local node rank
     */
    public Optional<NodeRank> findFullyConnectedNode(String endpoint) {
        log.trace("Find responsive node. Unresponsive nodes: {}", unresponsiveNodes);

        NodeConnectivity node = getNodeConnectivity(endpoint);

        switch (node.getType()) {
            case NOT_READY:
            case UNAVAILABLE:
                log.trace("Not connected node: {}", endpoint);
                return Optional.empty();
            case CONNECTED:
                for (String adjacent : node.getConnectivity().keySet()) {
                    //if adjacent node is unresponsive then exclude it from fully connected graph
                    if (unresponsiveNodes.contains(adjacent)) {
                        continue;
                    }

                    NodeConnectivity adjacentNode = getNodeConnectivity(adjacent);

                    //if adjacent node is unavailable then exclude it from  fully connected graph
                    if (adjacentNode.getType() == NodeConnectivityType.UNAVAILABLE) {
                        continue;
                    }

                    if (adjacentNode.getConnectionStatus(endpoint) != ConnectionStatus.OK) {
                        log.trace("Fully connected node not found");
                        return Optional.empty();
                    }
                }

                NodeRank rank = new NodeRank(
                        endpoint,
                        getNodeConnectivity(endpoint).getConnected()
                );

                return Optional.of(rank);
            default:
                throw new IllegalStateException("Unknown node state");
        }
    }

    /**
     * Get node by name
     *
     * @param node node name
     * @return a node in the graph
     */
    @VisibleForTesting
    public NodeConnectivity getNodeConnectivity(String node) {
        return graph.get(node);
    }

    /**
     * Graph size
     *
     * @return size
     */
    public int size() {
        return graph.size();
    }

    public String toJson() {
        return JsonUtils.toJson(connectivityGraph());
    }

    /**
     * Transform ClusterGraph to Connectivity graph
     *
     * @return connectivity graph
     */
    public ConnectivityGraph connectivityGraph() {
        return new ConnectivityGraph(new TreeSet<>(graph.values()));
    }

    private NavigableSet<NodeRank> getNodeRanks() {
        NavigableSet<NodeRank> nodes = new TreeSet<>();

        graph.keySet().forEach(node -> {
            int numConnected = graph.get(node).getConnected();
            nodes.add(new NodeRank(node, numConnected));
        });

        return nodes;
    }

    /**
     * Get connection status between two nodes in the graph.
     * For instance:
     * {a: [{a: OK}, {b: FAILED}]}
     * {b: [{a: OK}, {b: FAILED}]}
     * getConnectionStatus("a", "b") -> FAILED
     * getConnectionStatus("b", "a") -> OK
     *
     * @param sourceNode source node
     * @param targetNode second node
     * @return connection status
     */
    private ConnectionStatus getConnectionStatus(NodeConnectivity sourceNode, NodeConnectivity targetNode) {

        if (sourceNode == null || targetNode == null) {
            String errMsg = "Source or target node is null. Source: " + sourceNode + ", target: " + targetNode;
            throw new IllegalArgumentException(errMsg);
        }

        Set<NodeConnectivityType> types = EnumSet.of(sourceNode.getType(), targetNode.getType());
        if (types.contains(NodeConnectivityType.UNAVAILABLE)) {
            return ConnectionStatus.FAILED;
        }

        return sourceNode.getConnectionStatus(targetNode.getEndpoint());
    }

    /**
     * Helper provides methods to build cluster graph
     * <pre>
     * ClusterGraph graph = cluster(
     *     connectivity("a", ImmutableMap.of("a", OK, "b", FAILED, "c", OK)),
     *     unavailable("b"),
     *     connectivity("c", ImmutableMap.of("a", OK, "b", FAILED, "c", OK))
     * );
     * </pre>
     */
    public static class ClusterGraphHelper {

        private ClusterGraphHelper() {
            //prevent creating instances
        }

        public static ClusterGraph cluster(String localNode,
                                           ImmutableList<String> unresponsiveNodes,
                                           NodeConnectivity... nodes) {
            Map<String, NodeConnectivity> graph = Arrays.stream(nodes)
                    .collect(Collectors.toMap(NodeConnectivity::getEndpoint, Function.identity()));

            return ClusterGraph.builder()
                    .localNode(localNode)
                    .unresponsiveNodes(unresponsiveNodes)
                    .graph(ImmutableMap.copyOf(graph))
                    .build();
        }
    }
}
