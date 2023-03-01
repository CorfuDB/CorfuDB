package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import static org.corfudb.common.util.URLUtils.getVersionFormattedEndpointURL;

import java.util.Objects;

/**
 * This class represents a Log Replication Node
 */
@Slf4j
public class NodeDescriptor {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeDescriptor that = (NodeDescriptor) o;
        return Objects.equals(host, that.host) && Objects.equals(port, that.port) &&
                clusterId.equals(that.clusterId) && Objects.equals(connectionId, that.connectionId) &&
                nodeId.equals(that.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, clusterId, connectionId, nodeId);
    }

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

    public String getEndpoint() {
        // TODO: once Chetan's fix is merged, revisit to handle the exception
        if (host == null || port == null) {
            return "";
        }
        return getVersionFormattedEndpointURL(host, port);
    }

    @Override
    public String toString() {
        return String.format("Node: %s, %s", nodeId, getEndpoint());
    }

}
