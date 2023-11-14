package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
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
    
    /**
     * Get outgoing sessions
     * @return a set of sessions where the local cluster is a SOURCE
     */
    Set<LogReplicationSession> getOutgoingSessions();

    /**
     * Get incoming sessions
     * @return a set of sessions where the local cluster is a SINK
     */
    Set<LogReplicationSession> getIncomingSessions();

    /**
     * Gets the replication endpoint of the local cluster. Used only for the ITs.
     */
    @VisibleForTesting
    String getLocalEndpoint();
}
