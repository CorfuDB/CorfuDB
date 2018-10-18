package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.Builder;
import lombok.Data;

import org.corfudb.util.NodeLocator;

/**
 * NodeView is a node's view of the cluster. This is sent in response to a heartbeat request by a
 * Management Server. It stores its own endpoint and server metrics and also the network condition
 * of all its peers.
 *
 * <p>Created by zlokhandwala on 4/12/18.
 */
@Data
@Builder
public class NodeView implements ICorfuPayload<NodeView> {

    /**
     * Current node's Endpoint.
     */
    private final NodeLocator endpoint;

    /**
     * Current node's Server Metrics.
     */
    private final ServerMetrics serverMetrics;

    /**
     * Node's view of the cluster.
     */
    private final NetworkMetrics networkMetrics;

    public NodeView(NodeLocator endpoint,
                    ServerMetrics serverMetrics,
                    NetworkMetrics networkMetrics) {
        this.endpoint = endpoint;
        this.serverMetrics = serverMetrics;
        this.networkMetrics = networkMetrics;
    }

    public NodeView(ByteBuf buf) {
        endpoint = NodeLocator.parseString(ICorfuPayload.fromBuffer(buf, String.class));
        serverMetrics = ICorfuPayload.fromBuffer(buf, ServerMetrics.class);
        networkMetrics = ICorfuPayload.fromBuffer(buf, NetworkMetrics.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, endpoint.toString());
        ICorfuPayload.serialize(buf, serverMetrics);
        ICorfuPayload.serialize(buf, networkMetrics);
    }

    /**
     * Creates a default NodeView for the given endpoint.
     * This contains default ServerMetrics and default NetworkMetrics.
     *
     * @param endpoint Endpoint for the NodeView.
     * @return Default NodeView.
     */
    public static NodeView getDefaultNodeView(NodeLocator endpoint) {
        return new NodeView(endpoint,
                ServerMetrics.getDefaultServerMetrics(endpoint),
                NetworkMetrics.getDefaultNetworkMetrics());
    }
}
