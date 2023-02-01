package org.corfudb.infrastructure.logreplication.infrastructure;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.runtime.LogReplication;

import java.util.Map;
import java.util.UUID;

/**
 * This class contains all the interfaces exposed from the Discovery Service to the Cluster Manager plugin.
 */
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
    Map<LogReplication.LogReplicationSession, LogReplicationMetadata.ReplicationStatus> queryReplicationStatus();

    /**
     * Enforce snapshotFullSync
     */
    UUID forceSnapshotSync(LogReplication.LogReplicationSession session) throws LogReplicationDiscoveryServiceException;


    LogReplicationClusterInfo.ClusterRole getLocalClusterRoleType();
}
