package org.corfudb.util;

import lombok.Data;

/**
 * Defines an InterCluster Connection based on:
 * - Remote Site/Cluster unique identifier
 * - Remote Node locator
 * - Local Site/Cluster unique identifier
 */
@Data
public class InterClusterConnectionDescriptor {

    private NodeLocator remoteNodeLocator;

    private String remoteClusterId;

    private String localClusterId;

    public InterClusterConnectionDescriptor(NodeLocator remoteNodeLocator, String remoteClusterId, String localClusterId) {
        this.remoteNodeLocator = remoteNodeLocator;
        this.remoteClusterId = remoteClusterId;
        this.localClusterId = localClusterId;
    }
}
