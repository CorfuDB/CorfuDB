package org.corfudb.infrastructure.logreplication.infrastructure;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;

import java.util.Map;

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
    Map<String, LogReplicationMetadata.ReplicationStatusVal> queryReplicationStatus();

    void forceSnapshotFullSync(String clusterId);
}
