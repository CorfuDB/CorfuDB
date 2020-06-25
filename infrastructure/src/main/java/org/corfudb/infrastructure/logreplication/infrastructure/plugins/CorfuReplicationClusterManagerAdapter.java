package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;

import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryServiceAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;

interface CorfuReplicationSiteManagerAdapter {

    void setCorfuReplicationDiscoveryService(CorfuReplicationDiscoveryServiceAdapter corfuReplicationDiscoveryService);

    void setLocalEndpoint(String endpoint);

    // This is the currentTopology cached at the local node.
    public TopologyConfigurationMsg getTopologyConfig();

    // This will talk to the real Site Manager and get the most current topology.
    TopologyConfigurationMsg queryTopologyConfig();

    // This is called when get a notification of site config change.
    void updateTopologyConfig(TopologyConfigurationMsg newSiteConfigMsg);

    void start();

    void shutdown();

    void prepareSiteRoleChange();

    int queryReplicationStatus();
}
