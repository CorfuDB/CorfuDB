package org.corfudb.infrastructure.logreplication.infrastructure;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatus;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

import java.util.Map;
import java.util.UUID;

/**
 * This class contains all the interfaces exposed from the Discovery Service to the Cluster Manager plugin.
 */
public interface CorfuReplicationDiscoveryServiceAdapter {

    /**
     * Update with new topology
     * @param topologyConfiguration new topology
     */
    void updateTopology(TopologyDescriptor topologyConfiguration);

    /**
     * Query replication status
     * @return Map of session and its corresponding replication status
     */
    Map<LogReplicationSession, ReplicationStatus> queryReplicationStatus();

    /**
     * Enforce snapshotFullSync
     */
    UUID forceSnapshotSync(LogReplicationSession session) throws LogReplicationDiscoveryServiceException;

    // TODO [V2]: Remove this when localNodeId moves to plugin
    String getLocalNodeId();

    SessionManager getSessionManager();

}
