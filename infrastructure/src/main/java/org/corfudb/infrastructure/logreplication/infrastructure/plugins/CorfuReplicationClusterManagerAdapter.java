package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryServiceAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;

import java.util.Map;

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
     * This API enforce a full snapshot sync on the standby cluster with the clusterId at best effort.
     * The command can only be executed on the active cluster's node.
     *
     * @param clusterId
     */
    void forceSnapshotSync(String clusterId) throws LogReplicationDiscoveryServiceException;
}
