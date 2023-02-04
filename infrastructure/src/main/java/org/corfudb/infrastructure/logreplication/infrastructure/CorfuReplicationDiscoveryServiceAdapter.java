package org.corfudb.infrastructure.logreplication.infrastructure;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatus;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * This class contains all the interfaces exposed from the Discovery Service to the Cluster Manager plugin.
 */
public interface CorfuReplicationDiscoveryServiceAdapter {

    /**
     * Update with new topology
     * @param topologyConfiguration new topology
     */
    void updateTopology(TopologyConfigurationMsg topologyConfiguration);

    /**
     * Query replication status
     * @return Map of session and its corresponding replication status
     */
    Map<LogReplicationSession, ReplicationStatus> queryReplicationStatus();

    /**
     * Enforce snapshotFullSync
     */
    UUID forceSnapshotSync(LogReplicationSession session) throws LogReplicationDiscoveryServiceException;

    /**
     * Get incoming sessions
     * @return a set of sessions where the local cluster is a SINK
     */
    Set<LogReplicationSession> getIncomingSessions();

    ClusterRole getLocalClusterRoleType();
}
