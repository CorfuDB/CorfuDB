package org.corfudb.infrastructure.logreplication.infrastructure;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatus;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

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
    void updateTopology(TopologyConfigurationMsg topologyConfiguration);

    /**
     *
     * @return
     */
    Map<LogReplicationSession, ReplicationStatus> queryReplicationStatus();

    /**
     * Enforce snapshotFullSync
     */
    UUID forceSnapshotSync(LogReplicationSession session) throws LogReplicationDiscoveryServiceException;


    ClusterRole getLocalClusterRoleType();
}
