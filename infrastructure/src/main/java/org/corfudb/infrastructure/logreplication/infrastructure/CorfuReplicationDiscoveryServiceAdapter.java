package org.corfudb.infrastructure.logreplication.infrastructure;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;

public interface CorfuReplicationDiscoveryServiceAdapter {

    /**
     *
     * @param topologyConfiguration
     */
    void updateTopology(LogReplicationClusterInfo.TopologyConfigurationMsg topologyConfiguration);

    /**
     *
     */
    void prepareToBecomeStandby();

    /**
     *
     * @return
     */
    int queryReplicationStatus();
}
