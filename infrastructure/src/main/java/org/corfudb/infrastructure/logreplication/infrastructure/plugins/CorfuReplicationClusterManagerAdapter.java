package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;

import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryServiceAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;

public abstract class CorfuReplicationClusterManagerAdapter {
    @Getter
    CorfuReplicationDiscoveryServiceAdapter corfuReplicationDiscoveryService;

    @Getter
    TopologyConfigurationMsg topologyConfig;

    public void setCorfuReplicationDiscoveryService(CorfuReplicationDiscoveryServiceAdapter corfuReplicationDiscoveryService) {
        this.corfuReplicationDiscoveryService = corfuReplicationDiscoveryService;
        start();
    }

    public synchronized TopologyConfigurationMsg fetchTopology() {
        topologyConfig = queryTopologyConfig();
        return topologyConfig;
    }

    /**
     * Will be called when the cluster change and a new configuration is sent over
     *
     * @param newTopologyConfigMsg
     */
    public synchronized void updateTopologyConfig(TopologyConfigurationMsg newTopologyConfigMsg) {
        if (newTopologyConfigMsg.getTopologyConfigID() > topologyConfig.getTopologyConfigID()) {
            topologyConfig = newTopologyConfigMsg;
            corfuReplicationDiscoveryService.updateSiteConfig(topologyConfig);
        }
    }

    public void prepareSiteRoleChange() {
        corfuReplicationDiscoveryService.prepareSiteRoleChange();
    }

    public int queryReplicationStatus() {
        return corfuReplicationDiscoveryService.queryReplicationStatus();
    }

    //TODO: handle the case that queryTopologyConfig return an exception.
    public abstract TopologyConfigurationMsg queryTopologyConfig();

    public abstract void start();

    public abstract void shutdown();
}
