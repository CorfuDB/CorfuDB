package org.corfudb.infrastructure.management;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.NodeState.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.NodeState.NodeConnectivityState;

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
            if (adjacent.getType() == NodeConnectivityState.UNAVAILABLE) {
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

        if (first.numConnections < quorum) {
            log.error("No quorum to detect failed servers. Graph: {}, decision maker candidate: {}", this, first);
            return Optional.empty();
        }

        log.trace("Decision maker has found: {}", first);
        return Optional.of(first);
    }

    public Optional<NodeRank> findFailedNode() {
        log.trace("Get failed node");

        NavigableSet<NodeRank> nodes = getNodeRanks();
        if (nodes.isEmpty()) {
            log.error("Empty graph. Can't provide failed node");
            return Optional.empty();
        }

        NodeRank last = nodes.last();
        if (last.numConnections == graph.size()) {
            return Optional.empty();
        }

        return Optional.of(last);
    }

    public ImmutableMap<String, NodeRank> findFullyConnectedResponsiveNodes(List<String> unresponsiveNodes) {
        log.trace("Find responsive node. Unresponsive nodes: {}", unresponsiveNodes);

        //find all fully connected nodes, marked as unresponsive in the layout
        Map<String, NodeRank> responsiveNodes = new HashMap<>();
        for (String unresponsiveNode : unresponsiveNodes) {
            if (isFullyConnected(unresponsiveNode)) {
                responsiveNodes.put(unresponsiveNode, new NodeRank(unresponsiveNode, graph.size()));
            }
        }

        return ImmutableMap.copyOf(responsiveNodes);
    }

    private boolean isFullyConnected(String node) {
        for (NodeConnectivity currNode : graph.values()) {
            if (!currNode.getConnectionStatus(node)) {
                return false;
            }
        }

        return true;
    }

    @VisibleForTesting
    NodeConnectivity getNode(String node) {
        return graph.get(node);
    }

    public int size() {
        return graph.size();
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

        Set<NodeConnectivityState> types = EnumSet.of(source.getType(), target.getType());
        if (types.contains(NodeConnectivityState.UNAVAILABLE)) {
            return false;
        }

        return source.getConnectionStatus(targetNode);
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    @ToString
    public static class NodeRank implements Comparable<NodeRank> {
        private final String endpoint;
        private final int numConnections;

        @Override
        public int compareTo(NodeRank other) {
            //Descending order
            int connectionRank = Integer.compare(other.numConnections, numConnections);
            if (connectionRank != 0) {
                return connectionRank;
            }

            //Ascending order
            return endpoint.compareTo(other.endpoint);
        }

        public boolean is(String endpoint) {
            return this.endpoint.equals(endpoint);
        }
    }
}
