package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import org.corfudb.runtime.view.Layout;
import org.corfudb.util.NodeLocator;

/**
 * Contains a Node's state:
 * Sequencer state - ready/not_ready.
 * connectivity status - Node's connectivity with every other node in the layout.
 * Created by zlokhandwala on 11/2/18.
 */
@Data
@Builder
@AllArgsConstructor
public class NodeState implements ICorfuPayload<NodeState> {

    public static final long INVALID_HEARTBEAT_COUNTER = -1L;

    /**
     * Current node's Endpoint.
     */
    private final NodeLocator endpoint;

    private final long epoch;

    /**
     * Heartbeat counter.
     */
    private final long heartbeatCounter;

    /**
     * Sequencer metrics of the node.
     */
    private final SequencerMetrics sequencerMetrics;

    /**
     * Node's view of the cluster.
     */
    private final Map<String, Boolean> connectivityStatus;

    public NodeState(ByteBuf buf) {
        endpoint = NodeLocator.parseString(ICorfuPayload.fromBuffer(buf, String.class));
        epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        heartbeatCounter = ICorfuPayload.fromBuffer(buf, Long.class);
        sequencerMetrics = ICorfuPayload.fromBuffer(buf, SequencerMetrics.class);
        connectivityStatus = ICorfuPayload.mapFromBuffer(buf, String.class, Boolean.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, endpoint.toString());
        ICorfuPayload.serialize(buf, epoch);
        ICorfuPayload.serialize(buf, heartbeatCounter);
        ICorfuPayload.serialize(buf, sequencerMetrics);
        ICorfuPayload.serialize(buf, connectivityStatus);
    }

    /**
     * Creates a default NodeState for the given endpoint.
     * This contains default SequencerMetrics and empty connectivityStatus.
     *
     * @param endpoint Endpoint for the NodeState.
     * @return Default NodeState.
     */
    public static NodeState getDefaultNodeState(NodeLocator endpoint) {
        return new NodeState(endpoint,
                Layout.INVALID_EPOCH,
                INVALID_HEARTBEAT_COUNTER,
                SequencerMetrics.UNKNOWN,
                Collections.emptyMap());
    }
}
