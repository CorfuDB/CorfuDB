package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.NodeState.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.NodeState.NodeConnectivityState;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
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
public class ClusterGraph {
    private ImmutableMap<String, NodeConnectivity> graph;

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
     * Get nodes with a number of successful connections more than half in the cluster.
     *
     * @return sorted set of nodes
     */
    public SortedSet<NodeRank> getQuorumNodes() {
        SortedSet<NodeRank> nodes = new TreeSet<>();

        graph.keySet().forEach(node -> {
            int numConnected = graph.get(node).getConnected();
            if (numConnected > graph.size() / 2) {
                nodes.add(new NodeRank(node, numConnected));
            }
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

        return source.getNodeStatus(targetNode);
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @ToString
    static class NodeRank implements Comparable<NodeRank> {
        private final String endpoint;
        private final int numConnections;

        @Override
        public int compareTo(NodeRank other) {
            int connectionRank = Integer.compare(numConnections, other.numConnections);
            if (connectionRank != 0) {
                return connectionRank;
            }

            return endpoint.compareTo(other.endpoint);
        }

        public boolean is(String endpoint) {
            return this.endpoint.equals(endpoint);
        }
    }
}
