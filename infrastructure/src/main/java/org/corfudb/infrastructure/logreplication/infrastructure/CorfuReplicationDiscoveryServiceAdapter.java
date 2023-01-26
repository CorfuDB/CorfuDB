package org.corfudb.infrastructure.logreplication.infrastructure;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;

import java.util.Map;
import java.util.UUID;

public interface CorfuReplicationDiscoveryServiceAdapter {

    /**
     * update the topology configuration
     * @param topologyConfiguration
     */
    void updateTopology(LogReplicationClusterInfo.TopologyConfigurationMsg topologyConfiguration);

    /**
     * Retrieves replication status from LR metadata tables
     * @return Map of remoteClusterID to replicationStatus
     */
    Map<String, LogReplicationMetadata.ReplicationStatusVal> queryReplicationStatus();

    /**
     * Enforce snapshotFullSync
     * @return force snapshot sync ID
     */
    UUID forceSnapshotSync(String clusterId) throws LogReplicationDiscoveryServiceException;

    /**
     * Get the role of local cluster if appropriate
     * @return Role
     */
    LogReplicationClusterInfo.ClusterRole getLocalClusterRoleType();

    /**
     * Get local cluster descriptor
     * @return instance of ClusterDescriptor
     */
    ClusterDescriptor getLocalCluster();

    /**
     * Get local node ID
     * @return local node ID
     */
    String getLocalNodeId();

}
