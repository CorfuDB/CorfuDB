package org.corfudb.infrastructure.management.failuredetector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.failuredetector.FailureDetectorMetrics.ConnectivityGraph;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.NodeConnectivityType;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.util.JsonUtils;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Graph representation of a cluster state. For instance, asymmetric graph,
 * there is an asymmetric failure between nodes `a` and `b`:
 * {a -> {{a: true}, {b: false}, {c: true}}}
 * {b -> {{a: false}, {b: true}, {c: false}}}
 * {c -> {{a: true}, {b: false}, {c: true}}}
 */
@Builder
@ToString
@Slf4j
public class ClusterGraph {
    private ImmutableMap<String, NodeConnectivity> graph;

    /**
     * Transform a cluster state to the cluster graph
     *
     * @param cluster cluster state
     * @return cluster graph
     */
    public static ClusterGraph transform(ClusterState cluster) {
        Map<String, NodeConnectivity> graph = cluster.getNodes()
                .values()
                .stream()
                .map(NodeState::getConnectivity)
                .collect(Collectors.toMap(NodeConnectivity::getEndpoint, Function.identity()));

        return ClusterGraph.builder().graph(ImmutableMap.copyOf(graph)).build();
    }

    /**
     * Convert a cluster graph which could have asymmetric failures to a graph with symmetric failures between nodes.
     *
     * @return a graph with symmetric failures between nodes.
     */
    public ClusterGraph toSymmetric() {
        Map<String, NodeConnectivity> symmetric = new HashMap<>();

        graph.keySet().forEach(node -> {
            NodeConnectivity adjacent = graph.get(node);
            if (adjacent.getType() == NodeConnectivityType.UNAVAILABLE) {
                symmetric.put(node, adjacent);
                return;
            }

            ImmutableMap.Builder<String, Boolean> newConnectivity = ImmutableMap.builder();
            adjacent.getConnectivity().keySet().forEach(adjNode -> {
                boolean status = getConnectionStatus(node, adjNode);
                boolean oppositeStatus = getConnectionStatus(adjNode, node);

                newConnectivity.put(adjNode, status && oppositeStatus);
            });

            NodeConnectivity symmetricConnectivity = NodeConnectivity.builder()
                    .endpoint(node)
                    .connectivity(newConnectivity.build())
                    .build();

            symmetric.put(node, symmetricConnectivity);
        });

        return ClusterGraph.builder().graph(ImmutableMap.copyOf(symmetric)).build();
    }

    /**
     * Get a decision maker node to detect a failure. It must have:
     * - highest number of successful connections in the graph
     * - quorum of connected nodes to be able to make a decision (to make a node failed)
     *
     * @return sorted set of nodes
     */
    public Optional<NodeRank> getDecisionMaker() {
        log.trace("Get decision maker");

        NavigableSet<NodeRank> nodes = getNodeRanks();

        if (nodes.isEmpty()) {
            log.error("Empty graph. Can't provide decision maker");
            return Optional.empty();
        }

        int quorum = graph.size() / 2 + 1;
        NodeRank first = nodes.first();

        if (first.getNumConnections() < quorum) {
            log.error("No quorum to detect failed servers. Graph: {}, decision maker candidate: {}", this, first);
            return Optional.empty();
        }

        log.trace("Decision maker has found: {}, all node ranks: {}", first, nodes);
        return Optional.of(first);
    }

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

        return Optional.of(last);
    }

    public Optional<NodeRank> findFullyConnectedResponsiveNode(String localEndpoint, List<String> unresponsiveNodes) {
        log.trace("Find responsive node. Unresponsive nodes: {}", unresponsiveNodes);

        if (!unresponsiveNodes.contains(localEndpoint)) {
            log.trace("Local node is responsive. Nothing to heal");
            return Optional.empty();
        }

        NodeConnectivity localNode = getNode(localEndpoint);
        for (String adjacent : localNode.getConnectivity().keySet()) {
            if (unresponsiveNodes.contains(adjacent)) {
                continue;
            }

            NodeConnectivity adjacentNode = getNode(adjacent);

            if (adjacentNode.getType() == NodeConnectivityType.UNAVAILABLE) {
                continue;
            }

            if (!adjacentNode.getConnectionStatus(localEndpoint)) {
                log.trace("Fully connected node not found");
                return Optional.empty();
            }
        }

        return Optional.of(new NodeRank(localEndpoint, getNode(localEndpoint).getConnected()));
    }

    @VisibleForTesting
    public NodeConnectivity getNode(String node) {
        return graph.get(node);
    }

    public int size() {
        return graph.size();
    }

    public String toJson() {
        return JsonUtils.toJson(this);
    }

    public ConnectivityGraph connectivityGraph(){
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

    private boolean getConnectionStatus(String sourceNode, String targetNode) {
        NodeConnectivity source = graph.get(sourceNode);
        NodeConnectivity target = graph.get(targetNode);

        Set<NodeConnectivityType> types = EnumSet.of(source.getType(), target.getType());
        if (types.contains(NodeConnectivityType.UNAVAILABLE)) {
            return false;
        }

        return source.getConnectionStatus(targetNode);
    }

    /**
     * Helper provides methods to build cluster graph
     * <pre>
     * ClusterGraph graph = cluster(
     *     connectivity("a", ImmutableMap.of("a", true, "b", false, "c", true)),
     *     unavailable("b"),
     *     connectivity("c", ImmutableMap.of("a", true, "b", false, "c", true))
     * );
     * </pre>
     */
    public static class ClusterGraphHelper {

        private ClusterGraphHelper(){
            //prevent creating instances
        }

        public static ClusterGraph cluster(NodeConnectivity... nodes) {
            Map<String, NodeConnectivity> graph = Arrays.stream(nodes)
                    .collect(Collectors.toMap(NodeConnectivity::getEndpoint, Function.identity()));

            return ClusterGraph.builder()
                    .graph(ImmutableMap.copyOf(graph))
                    .build();
        }

        public static NodeConnectivity connectivity(String endpoint, ImmutableMap<String, Boolean> state) {
            return NodeConnectivity.builder()
                    .endpoint(endpoint)
                    .type(NodeConnectivityType.CONNECTED)
                    .connectivity(state)
                    .build();
        }

        public static NodeConnectivity unavailable(String endpoint) {
            return NodeConnectivity.builder()
                    .endpoint(endpoint)
                    .type(NodeConnectivityType.UNAVAILABLE)
                    .connectivity(ImmutableMap.of())
                    .build();
        }
    }
}
