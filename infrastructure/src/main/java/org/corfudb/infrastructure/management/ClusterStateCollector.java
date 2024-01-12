package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus;
import org.corfudb.runtime.exceptions.WrongEpochException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Collects information about cluster state.
 * Provides list of servers in the cluster with higher epochs.
 * Builds the {@link ClusterState} and local {@link NodeState}.
 * <p>
 * The collector navigates via list of requests represented as CompletableFuture-s
 * and collecting information about cluster.
 */
@Builder
@Slf4j
public class ClusterStateCollector {
    @NonNull
    private final String localEndpoint;
    @NonNull
    private final Map<String, CompletableFuture<NodeState>> clusterState;

    @NonNull
    private final FileSystemStats localNodeFileSystem;

    /**
     * Provides cluster state
     *
     * @param sequencerMetrics sequencer metrics
     * @return cluster state
     */
    public ClusterState collectClusterState(
            ImmutableList<String> unresponsiveNodes, SequencerMetrics sequencerMetrics) {

        Map<String, NodeState> nodeStates = new HashMap<>();

        nodeStates.put(localEndpoint, collectLocalNodeState(sequencerMetrics));
        nodeStates.putAll(collectRemoteStates());

        return ClusterState.builder()
                .localEndpoint(localEndpoint)
                .nodes(ImmutableMap.copyOf(nodeStates))
                .unresponsiveNodes(unresponsiveNodes)
                .build();
    }

    /**
     * Provides list of servers containing different epochs to this server.
     *
     * @return map of endpoint:epoch entries
     */
    public ImmutableMap<String, Long> collectWrongEpochs() {
        Map<String, Long> wrongEpochs = new HashMap<>();

        clusterState.forEach((server, state) -> {
            try {
                //Add a node to connectedNodes list if node responded with its NodeState
                state.get();
            } catch (Exception e) {
                //Collect wrong epoch, don't add the node to a failedNodes list
                if (e.getCause() instanceof WrongEpochException) {
                    wrongEpochs.put(server, ((WrongEpochException) e.getCause()).getCorrectEpoch());
                }
            }
        });

        return ImmutableMap.copyOf(wrongEpochs);
    }

    private Map<String, NodeState> collectRemoteStates() {
        Map<String, NodeState> nodeStates = new HashMap<>();
        //Collect information about cluster (exclude local node):
        clusterState.forEach((server, state) -> {
            if (server.equals(localEndpoint)) {
                return;
            }

            try {
                //Add a node to connectedNodes list if node responded with its NodeState
                NodeState nodeState = state.get();

                //Ignore local node response. Local NodeState will be built lately
                nodeStates.put(server, nodeState);
            } catch (Exception e) {
                //Add unavailable NodeState in the list if an exception happened. Ignore for localhost
                nodeStates.put(server, NodeState.getUnavailableNodeState(server));
            }
        });

        return nodeStates;
    }

    private NodeState collectLocalNodeState(SequencerMetrics sequencerMetrics) {
        log.trace("Get local node state");

        Map<String, ConnectionStatus> localNodeConnections = new HashMap<>();
        clusterState.forEach((server, state) -> {
            try {
                //Add a node to connectedNodes list if node responded with its NodeState
                state.get();
                localNodeConnections.put(server, ConnectionStatus.OK);
            } catch (Exception e) {
                ConnectionStatus nodeStatus;
                if (e.getCause() instanceof WrongEpochException) {
                    nodeStatus = ConnectionStatus.OK;
                } else {
                    nodeStatus = ConnectionStatus.FAILED;
                }

                localNodeConnections.put(server, nodeStatus);
            }
        });

        //Build local NodeState based on pings.
        NodeConnectivity localConnectivity = NodeConnectivity.connectivity(
                localEndpoint, ImmutableMap.copyOf(localNodeConnections)
        );

        return NodeState.builder()
                .connectivity(localConnectivity)
                .sequencerMetrics(sequencerMetrics)
                .fileSystem(Optional.of(localNodeFileSystem))
                .build();
    }
}
