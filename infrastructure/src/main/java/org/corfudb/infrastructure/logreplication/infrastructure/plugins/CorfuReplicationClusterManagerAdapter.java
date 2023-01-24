package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryServiceAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.runtime.LogReplication;

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
     *   Register the discovery service
     */
    void register(CorfuReplicationDiscoveryServiceAdapter corfuReplicationDiscoveryService);

     /**
     * Set the localEndpoint
     */
    void setLocalEndpoint(String endpoint);

    /**
     * Query the topology information.
     * @param useCached if it is true, used the cached topology, otherwise do a query to get the most
     *                  recent topology from the real Cluster Manager.
     * @return
     */
    TopologyConfigurationMsg queryTopologyConfig(boolean useCached);

    // This is called when get a notification of cluster config change.
    void updateTopologyConfig(TopologyConfigurationMsg newClusterConfigMsg);

    // start to talk to the upper layer to get cluster topology information
    void start();

    // Stop the ClusterManager service
    void shutdown();

    /**
     * This API is used to query the log replication status when it is preparing a role type flip and
     * the replicated tables should be in read-only mode.
     *
     * @return
     */
    Map<String, LogReplicationMetadata.ReplicationStatusVal> queryReplicationStatus();

    /**
     * This API enforce a full snapshot sync on the sink cluster with the clusterId at best effort.
     * The command can only be executed on the source cluster's node.
     *
     * @param clusterId
     */
    UUID forceSnapshotSync(String clusterId) throws LogReplicationDiscoveryServiceException;

    /**
     * This API is used to fetch the remote SOURCE clusters (w.r.t local cluster) and the supported replication models against it.
     *
     * @return Map of remote cluster that acts as SOURCE for a corresponding replication model.
     */
    Map<LogReplicationClusterInfo.ClusterConfigurationMsg, Set<LogReplicationMetadata.ReplicationModel>> getRemoteSourceToReplicationModels();

    /**
     * This API is used to fetch the remote SINK clusters (w.r.t local cluster) and the supported replication models against it.
     *
     * @return Map of remote cluster that acts as SINK for a corresponding replication model.
     */
    Map<LogReplicationClusterInfo.ClusterConfigurationMsg, Set<LogReplicationMetadata.ReplicationModel>> getRemoteSinkToReplicationModels();

    /**
     * This API is used to fetch the remote clusters that are the connection endpoints for the local cluster.
     *
     * @return ClusterConfigurationMsg
     */
    Set<LogReplicationClusterInfo.ClusterConfigurationMsg> fetchConnectionEndpoints();
}
