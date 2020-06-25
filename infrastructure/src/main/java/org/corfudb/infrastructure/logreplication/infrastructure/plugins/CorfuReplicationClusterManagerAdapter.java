package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryServiceAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;

/**
 * This is the interface for CorfuReplicationSiteManager.
 * Implementation of this should have following members:
 * 1. corfuReplicationDiscoveryService that is needed to notify the cluster configuration change.
 * 2. localEndpoint that has the local node information.
 *
 */
public interface CorfuReplicationClusterManagerAdapter {

    /**
     *   Set the corfuReplicationDiscoveryServiceMember
     */
     void setCorfuReplicationDiscoveryService(CorfuReplicationDiscoveryServiceAdapter corfuReplicationDiscoveryService);

     /**
     * Set the localEndpoint
     */
    void setLocalEndpoint(String endpoint);

    // This is the currentTopology cached at the adapter.
    public TopologyConfigurationMsg getTopologyConfig();

    // This will talk to the real Site Manager and get the most current topology.
    TopologyConfigurationMsg queryTopologyConfig();

    // This is called when get a notification of site config change.
    void updateTopologyConfig(TopologyConfigurationMsg newSiteConfigMsg);

    // Start the siteManager service
    void start();

    // Stop the siteManger service
    void shutdown();


    /**
     * While doing a site flip, it is the API used to notify the current log
     * replication node to prepare a site role type change. It will do some
     * bookkeeping to calculate the number of log entries to be sent over
     *
     */
    void prepareSiteRoleChange();

    // While preparing a site filp, the application can query the log replication
    // status and do a smooth transition till the querry

    /**
     * This API is used to query the log replication status when it is preparing a site flip and
     * the replicated tables should be in read-only mode.
     *
     * @return
     */
    int queryReplicationStatus();
}
