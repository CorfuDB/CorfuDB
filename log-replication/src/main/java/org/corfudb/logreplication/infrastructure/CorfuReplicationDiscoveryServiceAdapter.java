package org.corfudb.logreplication.infrastructure;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;

public interface CorfuReplicationDiscoveryServiceAdapter {
    /**
     *
     * @param topologyConfiguration
     */
    void updateSiteConfig(LogReplicationClusterInfo.TopologyConfigurationMsg topologyConfiguration);

    /**
     *
     */
    void prepareSiteRoleChange();

    /**
     *
     * @return
     */
    int queryReplicationStatus();
}
