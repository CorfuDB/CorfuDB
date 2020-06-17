package org.corfudb.infrastructure.logreplication.cluster;

import lombok.Getter;
import lombok.Setter;

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

    @Getter
    String clusterId;

    @Getter
    ClusterRole role;

    @Getter
    @Setter
    NodeDescriptor leader;

    @Getter
    List<NodeDescriptor> nodesDescriptors;

    public ClusterDescriptor(ClusterConfigurationMsg clusterConfig) {
        this.clusterId = clusterConfig.getId();
        this.role = clusterConfig.getRole();
        this.nodesDescriptors = new ArrayList<>();
        for (NodeConfigurationMsg nodeConfig : clusterConfig.getNodeInfoList()) {
            NodeDescriptor newNode = new NodeDescriptor(nodeConfig.getAddress(),
                    Integer.toString(nodeConfig.getPort()), clusterConfig.getRole(), Integer.toString(nodeConfig.getCorfuPort()),
                    clusterId, UUID.fromString(nodeConfig.getUuid()));
            this.nodesDescriptors.add(newNode);
        }
    }

    public ClusterDescriptor(ClusterDescriptor info, ClusterRole roleType) {
        this.clusterId = info.clusterId;
        this.role = roleType;
        this.leader = info.leader;
        this.nodesDescriptors = new ArrayList<>();
        for (NodeDescriptor nodeInfo : info.nodesDescriptors) {
            NodeDescriptor newNode = new NodeDescriptor(nodeInfo.getIpAddress(), nodeInfo.getPort(),
                    roleType, nodeInfo.getCorfuPort(), info.clusterId, nodeInfo.getNodeId());
            this.nodesDescriptors.add(newNode);
        }
    }

    public ClusterDescriptor(String siteId, ClusterRole roleType) {
        this.clusterId = siteId;
        this.role = roleType;
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
                .addAllNodeInfo(nodeInfoMsgs)
                .build();
        return clusterMsg;
    }

    @Override
    public String toString() {
        return String.format("Cluster[%s] --- Nodes[%s]:  %s", getClusterId(), nodesDescriptors.size(), nodesDescriptors);
    }
}
