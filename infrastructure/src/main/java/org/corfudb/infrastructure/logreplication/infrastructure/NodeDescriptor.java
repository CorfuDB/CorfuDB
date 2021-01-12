package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.NodeConfigurationMsg;

import java.util.UUID;

/**
 * This class represents a Log Replication Node
 */
@Slf4j
public class NodeDescriptor {

    @Getter
    private final String host;

    @Getter
    private final String port;

    @Getter
    private final String clusterId;

    // TODO: rename to connectionIdentifier (clean up nodeId and realNodeId) not changing to not break upper layers
    @Getter
    private final UUID nodeId;            // Connection Identifier (APH UUID in the case of NSX, used by IClientChannelAdapter)

    @Getter
    private final UUID realNodeId;        // Represents the node's identifier as tracked by the Topology Provider

    public NodeDescriptor(String host, String port, String siteId, UUID nodeId, UUID realNodeId) {
        this.host = host;
        this.port = port;
        this.clusterId = siteId;
        this.nodeId = nodeId;
        this.realNodeId = realNodeId;
    }

    public NodeConfigurationMsg convertToMessage() {
        NodeConfigurationMsg nodeConfig = NodeConfigurationMsg.newBuilder()
                .setAddress(host)
                .setPort(Integer.parseInt(port))
                .setUuid(nodeId.toString())
                .setNodeId(realNodeId.toString()).build();
        return nodeConfig;
    }

    public String getEndpoint() {
        return host + ":" + port;
    }

    @Override
    public String toString() {
        return String.format("Node: %s, %s", realNodeId, getEndpoint());
    }

}
