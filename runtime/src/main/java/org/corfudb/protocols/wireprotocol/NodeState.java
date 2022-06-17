package org.corfudb.protocols.wireprotocol;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.NodeConnectivityType;

import java.util.Optional;

/**
 * Contains a Node's state:
 * Sequencer state - ready/not_ready.
 * connectivity status - Node's connectivity with every other node in the layout.
 * <p>
 * For instance, node a fully connected to all nodes:
 * {"a": {"endpoint": "a", "connectivity":{"a": true, "b": true, "c": true}}}
 * <p>
 * Created by zlokhandwala on 11/2/18.
 */
@Getter
@Builder
@ToString
@AllArgsConstructor
@EqualsAndHashCode
public class NodeState {

    @NonNull
    private final NodeConnectivity connectivity;

    @NonNull
    private final Optional<FileSystemStats> fileSystem;
    /**
     * Sequencer metrics of the node.
     */
    @NonNull
    private final SequencerMetrics sequencerMetrics;

    public static NodeState getUnavailableNodeState(String endpoint){
        NodeConnectivity unavailable = NodeConnectivity.unavailable(endpoint);
        return new NodeState(unavailable, Optional.empty(), SequencerMetrics.UNKNOWN);
    }

    public static NodeState getNotReadyNodeState(String endpoint){
        NodeConnectivity notReady = NodeConnectivity.notReady(endpoint);
        return new NodeState(notReady, Optional.empty(), SequencerMetrics.UNKNOWN);
    }

    public boolean isConnected() {
        return connectivity.getType() == NodeConnectivityType.CONNECTED;
    }
}
