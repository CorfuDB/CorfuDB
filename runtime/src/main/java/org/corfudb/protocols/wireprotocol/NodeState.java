package org.corfudb.protocols.wireprotocol;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.NodeConnectivityType;

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

    private final NodeConnectivity connectivity;


    /**
     * Sequencer metrics of the node.
     */
    private final SequencerMetrics sequencerMetrics;

    public static NodeState getUnavailableNodeState(String endpoint){
        return new NodeState(
                NodeConnectivity.unavailable(endpoint),
                SequencerMetrics.UNKNOWN
        );
    }

    public static NodeState getNotReadyNodeState(String endpoint){
        return new NodeState(
                NodeConnectivity.notReady(endpoint),
                SequencerMetrics.UNKNOWN
        );
    }

    public boolean isConnected() {
        return connectivity.getType() == NodeConnectivityType.CONNECTED;
    }
}
