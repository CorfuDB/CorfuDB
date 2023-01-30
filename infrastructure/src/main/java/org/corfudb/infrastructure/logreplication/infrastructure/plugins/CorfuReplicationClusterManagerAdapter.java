package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SinkReplicationStatus;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatus;

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
    void register(CorfuReplicationDiscoveryService corfuReplicationDiscoveryService);

     /**
     * Set the localEndpoint
     */
    void setLocalEndpoint(String endpoint);

    /**
     * Query the topology information.
     * @param useCached if it is true, use the cached topology, otherwise do a query to get the most
     *                  recent topology from the Cluster Manager/Topology Provider.
     * @return
     */
    TopologyConfigurationMsg queryTopologyConfig(boolean useCached);

    /**
     * Callback to update topology on cluster changes
     */
    void updateTopologyConfig(TopologyConfigurationMsg newClusterConfig);

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
     * Query replication status on sink, currently only relevant status on sink is the consistency flag
     *  on sink
     */
    Map<LogReplicationSession, SinkReplicationStatus> queryStatusOnSink();

    /**
     * This API enforce a full snapshot sync on the sink cluster with the clusterId at best effort.
     * The command can only be executed on the source cluster's node.
     *
     * @param clusterId
     */
    // TODO(V2): this API needs to be modified to specify a LogReplicationSession (changes on client required)
    UUID forceSnapshotSync(String clusterId) throws LogReplicationDiscoveryServiceException;
}
