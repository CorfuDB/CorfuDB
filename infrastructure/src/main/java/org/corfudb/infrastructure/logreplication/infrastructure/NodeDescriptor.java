package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
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

    @Getter
    private final UUID nodeId;        // Connection Identifier (APH UUID in the case of NSX)

    public NodeDescriptor(String host, String port, String siteId, UUID nodeId) {
        this.host = host;
        this.port = port;
        this.clusterId = siteId;
        this.nodeId = nodeId;
    }

    public NodeConfigurationMsg convertToMessage() {
        NodeConfigurationMsg nodeConfig = NodeConfigurationMsg.newBuilder()
                .setAddress(host)
                .setPort(Integer.parseInt(port))
                .setUuid(nodeId.toString()).build();
        return nodeConfig;
    }

    public String getEndpoint() {
        return host + ":" + port;
    }

    @Override
    public String toString() {
        return String.format("Node: %s, %s", nodeId, getEndpoint());
    }

}
