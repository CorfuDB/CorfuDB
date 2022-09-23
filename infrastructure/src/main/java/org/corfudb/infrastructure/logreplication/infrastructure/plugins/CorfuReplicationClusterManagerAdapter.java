package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryServiceAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationStatus;

import java.util.Map;
import java.util.UUID;

/**
 * This is the interface for CorfuReplicationClusterManager.
 * Implementation of this should have following members:
 * 1. corfuReplicationDiscoveryService that is needed to notify the cluster configuration change.
 * 2. localEndpoint that has the local node information.
 *
 */
public interface CorfuReplicationClusterManagerAdapter {

    /**
     * Register the discovery service
     */
    void register(CorfuReplicationDiscoveryServiceAdapter corfuReplicationDiscoveryServiceAdapter);

    /**
     * Query the topology information.
     * @param useCached if it is true, use the cached topology, otherwise do a query to get the most
     *                  recent topology from the Cluster Manager/Topology Provider.
     * @return
     */
    TopologyDescriptor queryTopologyConfig(boolean useCached);

    /**
     * Callback to update topology on cluster changes
     */
    void updateTopologyConfig(TopologyDescriptor newClusterConfig);

    /**
     * Start cluster discovery against external topology provider
     */
    void start();

    /**
     * Shutdown cluster manager
     */
    void shutdown();

    /**
     * Query replication status for all ongoing sessions on source.
     * This API is primarily used for UI display of metadata or in preparation for role switchover.
     *
     * @return map of sessions to replication status
     */
    Map<LogReplicationSession, ReplicationStatus> queryReplicationStatus();

    /**
     * This API enforces a full snapshot sync on a session at best effort.
     * The command will be executed on a node in the source cluster.
     *
     * @param session
     */
    UUID forceSnapshotSync(LogReplicationSession session) throws LogReplicationDiscoveryServiceException;

    /**
     * This API enforce a full snapshot sync on the sink cluster with the clusterId at best effort.
     * The command can only be executed on the source cluster's node.
     *
     * @return node ID
     */
    String getLocalNodeId();
}
