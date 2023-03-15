package org.corfudb.infrastructure.logreplication.infrastructure;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.runtime.LogReplication;

import java.util.Map;
import java.util.UUID;

public interface CorfuReplicationDiscoveryServiceAdapter {

    /**
     *
     * @param topologyConfiguration
     */
    void updateTopology(LogReplicationClusterInfo.TopologyConfigurationMsg topologyConfiguration);

    /**
     *
     * @return
     */
    Map<String, LogReplication.ReplicationStatusVal> queryReplicationStatus();

    /**
     * Enforce snapshotFullSync
     */
    UUID forceSnapshotSync(String clusterId) throws LogReplicationDiscoveryServiceException;


    LogReplicationClusterInfo.ClusterRole getLocalClusterRoleType();
}
