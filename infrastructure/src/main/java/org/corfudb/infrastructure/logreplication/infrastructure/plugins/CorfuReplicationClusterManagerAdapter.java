package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatus;

import java.util.Map;
import java.util.Set;
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
     * This API enforces a full snapshot sync on a session at best effort.
     * The command will be executed on a node in the source cluster.
     *
     * @param session
     */
    UUID forceSnapshotSync(LogReplicationSession session) throws LogReplicationDiscoveryServiceException;

    /**
     * This API is used to fetch the remote SOURCE clusters (w.r.t local cluster) and the corresponding replication models.
     *
     * @return Map of remote cluster to a set of corresponding replication models.
     */
    Map<LogReplicationClusterInfo.ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> getRemoteSourceToReplicationModels();

    /**
     * This API is used to fetch the remote SINK clusters (w.r.t local cluster) and the corresponding replication models..
     *
     * @return Map of remote cluster to a set of corresponding replication models.
     */
    Map<LogReplicationClusterInfo.ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> getRemoteSinkToReplicationModels();

    /**
     * This API is used to fetch the remote clusters to which the local cluster will initiate connection.
     *
     * @return Set of clusters that the local cluster has to initiate connection.
     */
    Set<LogReplicationClusterInfo.ClusterConfigurationMsg> fetchConnectionEndpoints();
}
