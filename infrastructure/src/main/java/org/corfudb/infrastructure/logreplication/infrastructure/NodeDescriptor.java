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

    @Setter
    @Getter
    private ClusterRole roleType;

    @Getter
    private String host;

    @Getter
    private String port;

    @Getter
    private String clusterId;

    @Getter
    private UUID nodeId;        // Connection Identifier (APH UUID in the case of NSX)

    public NodeDescriptor(String host, String port, ClusterRole roleType,
                          String siteId, UUID nodeId) {
        this.host = host;
        this.roleType = roleType;
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
        return String.format("Role Type: %s, %s", roleType, getEndpoint());
    }

}
