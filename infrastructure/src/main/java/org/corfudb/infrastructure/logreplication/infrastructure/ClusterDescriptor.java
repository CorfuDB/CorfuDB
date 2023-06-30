package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.corfudb.common.util.URLUtils.getVersionFormattedHostAddress;

/**
 * This class describes a Cluster ore Site in terms of its Log Replication Nodes
 */
public class ClusterDescriptor {

    @Getter
    final String clusterId;

    @Getter
    final List<NodeDescriptor> nodeDescriptors;

    @Getter
    private final int corfuPort;    // Port on which Corfu DB runs on this cluster

    @VisibleForTesting
    public ClusterDescriptor(String clusterId, int corfuPort, List<NodeDescriptor> nodes) {
        this.clusterId = clusterId;
        this.corfuPort = corfuPort;
        this.nodeDescriptors = nodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterDescriptor that = (ClusterDescriptor) o;
        return corfuPort == that.corfuPort && clusterId.equals(that.clusterId) &&
                nodeDescriptors.equals(that.nodeDescriptors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId, nodeDescriptors, corfuPort);
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
        return String.format("Cluster[%s]:: CorfuPort=%s, Nodes[%s]:  %s", getClusterId(), corfuPort,
                nodeDescriptors.size(), listToString(nodeDescriptors));
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
                return String.format("%s:%s", getVersionFormattedHostAddress(node.getHost()), node.getPort());
            }
        }
        return null;
    }
}
