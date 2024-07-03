package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.NodeConfigurationMsg;

import java.util.ArrayList;
import java.util.List;

import static org.corfudb.common.util.URLUtils.getVersionFormattedHostAddress;

/**
 * This class describes a Cluster ore Site in terms of its Log Replication Nodes
 */
public class ClusterDescriptor {

    private static int CORFU_PORT = 9000;

    @Getter
    String clusterId;

    @Getter
    ClusterRole role;

    @Getter
    List<NodeDescriptor> nodesDescriptors;

    @Getter
    private int corfuPort;    // Port on which Corfu DB runs on this cluster

    public ClusterDescriptor(String clusterId) {
        this.clusterId = clusterId;
    }

    public ClusterDescriptor(ClusterConfigurationMsg clusterConfig) {
        this.clusterId = clusterConfig.getId();
        this.role = clusterConfig.getRole();
        this.corfuPort = clusterConfig.getCorfuPort() != 0 ? clusterConfig.getCorfuPort() : CORFU_PORT;
        this.nodesDescriptors = new ArrayList<>();
        for (NodeConfigurationMsg nodeConfig : clusterConfig.getNodeInfoList()) {
            NodeDescriptor newNode = new NodeDescriptor(
                    getVersionFormattedHostAddress(nodeConfig.getAddress()),
                    Integer.toString(nodeConfig.getPort()), clusterId,
                    nodeConfig.getConnectionId(),
                    nodeConfig.getNodeId());
            this.nodesDescriptors.add(newNode);
        }
    }

    public ClusterDescriptor(ClusterDescriptor info, ClusterRole roleType) {
        this.clusterId = info.clusterId;
        this.role = roleType;
        this.nodesDescriptors = new ArrayList<>();
        this.corfuPort = info.getCorfuPort();
        for (NodeDescriptor nodeInfo : info.nodesDescriptors) {
            NodeDescriptor newNode = new NodeDescriptor(getVersionFormattedHostAddress(nodeInfo.getHost()), nodeInfo.getPort(),
                    info.clusterId, nodeInfo.getConnectionId(), nodeInfo.getNodeId());
            this.nodesDescriptors.add(newNode);
        }
    }

    public ClusterDescriptor(String clusterId, ClusterRole roleType, int corfuPort) {
        this.clusterId = clusterId;
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
        return String.format("Cluster[%s]:: role=%s, CorfuPort=%s, Nodes[%s]: %s", getClusterId(), role, corfuPort, nodesDescriptors.size(), nodesDescriptors);
    }

    /**
     * Get descriptor for a specific endpoint
     *
     * @param nodeId node's id
     * @return endpoint's node descriptor or null if it does not belong to this cluster.
     */
    public NodeDescriptor getNode(String nodeId) {
        for (NodeDescriptor node : nodesDescriptors) {
            if(node.getNodeId().equals(nodeId)) {
                return node;
            }
        }
        return null;
    }

    public String getEndpointByNodeId(String nodeId) {
        for (NodeDescriptor node : nodesDescriptors) {
            if(node.getNodeId().equals(nodeId)) {
                return String.format("%s:%s", node.getHost(), node.getPort());
            }
        }
        return null;
    }
}
