package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * This class describes a Cluster ore Site in terms of its Log Replication Nodes
 */
public class ClusterDescriptor {

    private static int CORFU_PORT = 9000;

    @Getter
    final String clusterId;

    @Getter
    final ClusterRole role;

    @Getter
    final List<NodeDescriptor> nodeDescriptors;

    @Getter
    final private int corfuPort;    // Port on which Corfu DB runs on this cluster


    public ClusterDescriptor(ClusterDescriptor info, ClusterRole roleType) {
        this.clusterId = info.clusterId;
        this.role = roleType;
        this.nodeDescriptors = new ArrayList<>();
        this.corfuPort = info.getCorfuPort();
        for (NodeDescriptor nodeInfo : info.nodeDescriptors) {
            NodeDescriptor newNode = new NodeDescriptor(nodeInfo.getHost(), nodeInfo.getPort(),
                    info.clusterId, nodeInfo.getConnectionId(), nodeInfo.getNodeId());
            this.nodeDescriptors.add(newNode);
        }
    }

    @VisibleForTesting
    public ClusterDescriptor(String clusterId, ClusterRole roleType, int corfuPort) {
        this.clusterId = clusterId;
        this.role = roleType;
        this.corfuPort = corfuPort;
        nodeDescriptors = new ArrayList<>();
    }

    public static String listToString(List<?> list) {
        String result = "";
        for (int i = 0; i < list.size(); i++) {
            result += "\n" + list.get(i);
        }
        return result;
    }

    @Override
    public String toString() {
        return String.format("Cluster[%s]:: role=%s, CorfuPort=%s, Nodes[%s]:  %s", getClusterId(), role, corfuPort, nodeDescriptors.size(), listToString(nodeDescriptors));
    }

    /**
     * Get descriptor for a specific endpoint
     *
     * @param nodeId node's id
     * @return endpoint's node descriptor or null if it does not belong to this cluster.
     */
    public NodeDescriptor getNode(String nodeId) {
        for (NodeDescriptor node : nodeDescriptors) {
            if(node.getNodeId().equals(nodeId)) {
                return node;
            }
        }
        return null;
    }

    public String getEndpointByNodeId(String nodeId) {
        for (NodeDescriptor node : nodeDescriptors) {
            if(node.getNodeId().equals(nodeId)) {
                return String.format("%s:%s", node.getHost(), node.getPort());
            }
        }
        return null;
    }

    public enum ClusterRole {
        INVALID,
        SOURCE,
        SINK,
        NONE,
    }
}
