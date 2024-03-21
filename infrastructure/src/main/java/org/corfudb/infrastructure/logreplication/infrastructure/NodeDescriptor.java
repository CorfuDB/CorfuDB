package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.NodeConfigurationMsg;

import static org.corfudb.common.util.URLUtils.getVersionFormattedEndpointURL;

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

    @Getter
    private final String connectionId;    // Connection Identifier (APH UUID in the case of NSX, used by IClientChannelAdapter)

    @Getter
    private final String nodeId;      // Represents the node's identifier as tracked by the Topology Provider

    public NodeDescriptor(String host, String port, String siteId, String connectionId, String nodeId) {
        this.host = host;
        this.port = port;
        this.clusterId = siteId;
        this.connectionId = connectionId;
        this.nodeId = nodeId;
    }

    public NodeConfigurationMsg convertToMessage() {
        NodeConfigurationMsg nodeConfig = NodeConfigurationMsg.newBuilder()
                .setAddress(host)
                .setPort(Integer.parseInt(port))
                .setConnectionId(connectionId)
                .setNodeId(nodeId).build();
        return nodeConfig;
    }

    public String getEndpoint() {
        return getVersionFormattedEndpointURL(host, port);
    }

    @Override
    public String toString() {
        return String.format("Node: %s, %s", nodeId, getEndpoint());
    }

}
