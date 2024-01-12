package org.corfudb.protocols.wireprotocol.failuredetector;

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a degree of a node (the number of connections it has to other nodes)
 * <a href="https://en.wikipedia.org/wiki/Degree_distribution">Degree Distribution</a>
 */
@Builder
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class NodeConnectivity implements Comparable<NodeConnectivity> {
    /**
     * Node name
     */
    @Getter
    @NonNull
    private final String endpoint;

    @Getter
    @NonNull
    private final NodeConnectivityType type;

    /**
     * Node connectivity information, contains connection status to other nodes
     */
    @Getter
    @NonNull
    private final ImmutableMap<String, ConnectionStatus> connectivity;

    @Getter
    private final long epoch;

    /**
     * Contains list of servers successfully connected with current node.
     */
    public Set<String> getConnectedNodes() {
        return connectivity
                .keySet()
                .stream()
                .filter(adjacent -> connectivity.get(adjacent) == ConnectionStatus.OK)
                .collect(Collectors.toSet());
    }

    /**
     * Contains list of servers disconnected from this node.
     * If the node A can't ping node B then node B will be added to failedNodes list.
     */
    public Set<String> getFailedNodes() {
        return connectivity
                .keySet()
                .stream()
                .filter(adjacent -> connectivity.get(adjacent) == ConnectionStatus.FAILED)
                .collect(Collectors.toSet());
    }

    /**
     * Returns node status: connected, disconnected
     *
     * @param node node name
     * @return node status
     */
    public ConnectionStatus getConnectionStatus(String node) {
        if (type == NodeConnectivityType.UNAVAILABLE) {

            String error = String.format(
                    "%s: failed to get connection status for %s. Connectivity: %s",
                    endpoint, node, connectivity);
            throw new IllegalStateException(error);
        }

        if (!connectivity.containsKey(node)) {
            String error = String.format(
                    "%s: opposite node not found for %s. Connectivity: %s",
                    endpoint, node, connectivity);
            throw new IllegalStateException(error);
        }

        return connectivity.get(node);
    }

    /**
     * Get number of nodes this node is connected to
     *
     * @return number of connected nodes
     */
    public int getConnected() {
        return connectivity.keySet().stream()
                .mapToInt(node -> connectivity.get(node) == ConnectionStatus.OK ? 1 : 0)
                .sum();
    }

    /**
     * Compare node connectivity's according to their endpoints
     *
     * @param other another node connectivity
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
     * or greater than the specified object.
     */
    @Override
    public int compareTo(NodeConnectivity other) {
        return endpoint.compareTo(other.endpoint);
    }

    /**
     * Factory method to build a node connectivity
     * @param endpoint node name
     * @param connectivity connectivity matrix
     * @return NodeConnectivity
     */
    public static NodeConnectivity connectivity(String endpoint, ImmutableMap<String, ConnectionStatus> connectivity) {
        return NodeConnectivity.builder()
                .endpoint(endpoint)
                .type(NodeConnectivityType.CONNECTED)
                .connectivity(connectivity)
                .build();
    }

    /**
     * Builds a new connectivity with unavailable state
     * @param endpoint node name
     * @return NodeConnectivity
     */
    public static NodeConnectivity unavailable(String endpoint) {
        return NodeConnectivity.builder()
                .endpoint(endpoint)
                .type(NodeConnectivityType.UNAVAILABLE)
                .connectivity(ImmutableMap.of())
                .build();
    }

    /**
     * Builds a new connectivity with NOT_READY state
     * @param endpoint node name
     * @return NodeConnectivity
     */
    public static NodeConnectivity notReady(String endpoint) {
        return NodeConnectivity.builder()
                .endpoint(endpoint)
                .type(NodeConnectivityType.NOT_READY)
                .connectivity(ImmutableMap.of())
                .build();
    }

    public enum NodeConnectivityType {
        /**
         * Current node is not ready yet, missing information about current node state.
         * Node is not bootstrapped and/or didn't ping other nodes yet.
         */
        NOT_READY,
        /**
         * Two nodes are connected
         */
        CONNECTED,
        /**
         * We are unable to get node state from the node (link failure between the nodes)
         */
        UNAVAILABLE
    }

    /**
     * Node connection status
     */
    public enum ConnectionStatus {
        /**
         * A node connected to another node
         */
        OK,
        /**
         * Two nodes disconnected form each other
         */
        FAILED;
    }
}
