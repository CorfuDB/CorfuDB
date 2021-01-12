package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryServiceAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;

import java.util.Map;
import java.util.UUID;

/***
 * This is the base class for CorfuReplicationClusterManagerAdapter and implements the basic functionality.
 * Any ClusterManger Adapter implementation should extend this class or implement the interface.
 *
 */
@Slf4j
public abstract class CorfuReplicationClusterManagerBaseAdapter implements CorfuReplicationClusterManagerAdapter {
    @Getter
    public CorfuReplicationDiscoveryServiceAdapter corfuReplicationDiscoveryService;

    @Getter
    public TopologyConfigurationMsg topologyConfig;

    public String localEndpoint;

    public void register(CorfuReplicationDiscoveryServiceAdapter corfuReplicationDiscoveryService) {
        this.corfuReplicationDiscoveryService = corfuReplicationDiscoveryService;
    }

    public void setLocalEndpoint(String endpoint) {
        this.localEndpoint = endpoint;
    }

    /**
     * Will be called when the cluster change and a new configuration is sent over
     *
     * @param newTopologyConfigMsg
     */
    public synchronized void updateTopologyConfig(TopologyConfigurationMsg newTopologyConfigMsg) {
        if (newTopologyConfigMsg.getTopologyConfigID() > topologyConfig.getTopologyConfigID()) {
            topologyConfig = newTopologyConfigMsg;
            corfuReplicationDiscoveryService.updateTopology(topologyConfig);
        }
    }

    public Map<String, LogReplicationMetadata.ReplicationStatusVal> queryReplicationStatus() {
        return corfuReplicationDiscoveryService.queryReplicationStatus();
    }

    @Override
    public UUID forceSnapshotSync(String clusterId) throws LogReplicationDiscoveryServiceException {
        return corfuReplicationDiscoveryService.forceSnapshotSync(clusterId);
    }
}
