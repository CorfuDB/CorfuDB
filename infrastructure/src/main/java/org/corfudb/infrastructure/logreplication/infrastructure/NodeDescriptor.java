package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.NodeConfigurationMsg;

import java.util.UUID;


@Slf4j
public class NodeDescriptor {

    @Setter
    @Getter
    private ClusterRole roleType;

    @Getter
    private String ipAddress;

    @Setter
    @Getter
    private String corfuPort;

    @Getter
    private String port;

    @Getter
    @Setter
    private boolean leader;

    @Getter
    private String clusterId;

    @Getter
    private UUID nodeId;        // Connection Identifier (APH UUID in the case of NSX)

    public NodeDescriptor(String ipAddress, String port, ClusterRole roleType, String corfuPortNum,
                          String siteId, UUID nodeId) {
        this.leader = false;
        this.ipAddress = ipAddress;
        this.roleType = roleType;
        this.port = port;
        this.corfuPort = corfuPortNum;
        this.clusterId = siteId;
        this.nodeId = nodeId;
    }

    public NodeConfigurationMsg convertToMessage() {
        NodeConfigurationMsg nodeConfig = NodeConfigurationMsg.newBuilder()
                .setAddress(ipAddress)
                .setPort(Integer.parseInt(port))
                .setCorfuPort(Integer.parseInt(corfuPort))
                .setUuid(nodeId.toString()).build();
        return nodeConfig;
    }

    public String getEndpoint() {
        return ipAddress + ":" + port;
    }

    public String getCorfuEndpoint() {
        return ipAddress + ":" + corfuPort;
    }

    @Override
    public String toString() {
        return String.format("Role Type: %s, %s", roleType, getEndpoint());
    }

}
