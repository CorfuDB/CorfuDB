package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.NodeConfigurationMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * This class describes a Cluster or Site in terms of its Log Replication Nodes
 */
public class ClusterDescriptor {

    private static int CORFU_PORT = 9000;

    @Getter
    String clusterId;

    @Getter
    ClusterRole role;

    @Getter
    List<NodeDescriptor> nodesDescriptors;

    // Port on which Corfu DB runs on this cluster
    @Getter
    int corfuPort;

    public ClusterDescriptor(ClusterConfigurationMsg clusterConfig) {
        this.clusterId = clusterConfig.getId();
        this.role = clusterConfig.getRole();
        this.corfuPort = clusterConfig.getCorfuPort();
        this.nodesDescriptors = new ArrayList<>();
        for (NodeConfigurationMsg nodeConfig : clusterConfig.getNodeInfoList()) {
            NodeDescriptor newNode = new NodeDescriptor(nodeConfig.getAddress(),
                    Integer.toString(nodeConfig.getPort()), clusterConfig.getRole(),
                    clusterId, UUID.fromString(nodeConfig.getUuid()));
            this.nodesDescriptors.add(newNode);
        }
    }

    public ClusterDescriptor(ClusterDescriptor info, ClusterRole roleType) {
        this.clusterId = info.clusterId;
        this.role = roleType;
        this.nodesDescriptors = new ArrayList<>();
        this.corfuPort = info.getCorfuPort();
        for (NodeDescriptor nodeInfo : info.nodesDescriptors) {
            NodeDescriptor newNode = new NodeDescriptor(nodeInfo.getIpAddress(), nodeInfo.getPort(),
                    roleType, info.clusterId, nodeInfo.getNodeId());
            this.nodesDescriptors.add(newNode);
        }
    }

    public ClusterDescriptor(String siteId, ClusterRole roleType, int corfuPort) {
        this.clusterId = siteId;
        this.role = roleType;
        this.corfuPort = corfuPort;
        nodesDescriptors = new ArrayList<>();
    }

    public ClusterConfigurationMsg convertToMessage() {
        ArrayList<NodeConfigurationMsg> nodeInfoMsgs = new ArrayList<>();
        for (NodeDescriptor nodeInfo : nodesDescriptors) {
            nodeInfoMsgs.add(nodeInfo.convertToMessage());
        }

        ClusterConfigurationMsg clusterMsg = ClusterConfigurationMsg.newBuilder()
                .setId(clusterId)
                .setRole(role)
                .setCorfuPort(corfuPort)
                .addAllNodeInfo(nodeInfoMsgs)
                .build();
        return clusterMsg;
    }

    @Override
    public String toString() {
        return String.format("Cluster[%s] - CorfuPort[%s] --- Nodes[%s]:  %s", getClusterId(), corfuPort, nodesDescriptors.size(), nodesDescriptors);
    }

    public NodeDescriptor getNode(String endpoint) {
        for (NodeDescriptor node : nodesDescriptors) {
            if(node.getEndpoint().equals(endpoint)) {
                return node;
            }
        }

        return null;
    }
}
