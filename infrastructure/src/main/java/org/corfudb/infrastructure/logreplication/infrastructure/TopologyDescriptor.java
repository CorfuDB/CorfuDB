package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterConfigurationMsg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * This class represents a view of a Multi-Cluster/Site Topology,
 * where one cluster is the active/primary and n cluster's are standby's (backups).
 */
@Slf4j
public class TopologyDescriptor {

    // Represents a state of the topology configuration (a topology epoch)
    @Getter
    private long topologyConfigId;

    @Getter
    private ClusterDescriptor activeCluster;

    @Getter
    private Map<String, ClusterDescriptor> standbyClusters;

    /**
     * Constructor.
     *
     * @param topologyMessage
     */
    public TopologyDescriptor(TopologyConfigurationMsg topologyMessage) {
        this.topologyConfigId = topologyMessage.getTopologyConfigID();
        this.standbyClusters = new HashMap<>();
        for (ClusterConfigurationMsg clusterConfig : topologyMessage.getClustersList()) {
            ClusterDescriptor cluster = new ClusterDescriptor(clusterConfig);
            if (clusterConfig.getRole() == ClusterRole.ACTIVE) {
                activeCluster = cluster;
            } else if (clusterConfig.getRole() == ClusterRole.STANDBY) {
                addStandbyCluster(cluster);
            }
        }
    }

    public TopologyDescriptor(long topologyConfigId, ClusterDescriptor activeCluster, Map<String, ClusterDescriptor> standbyClusters) {
        this.topologyConfigId = topologyConfigId;
        this.activeCluster = activeCluster;
        this.standbyClusters = standbyClusters;
    }

    public TopologyConfigurationMsg convertToMessage() {
        log.info("descriptor {}", this);
        ArrayList<ClusterConfigurationMsg> clustersConfigs = new ArrayList<>();
        clustersConfigs.add((activeCluster.convertToMessage()));

        for (ClusterDescriptor siteInfo : standbyClusters.values()) {
            clustersConfigs.add(siteInfo.convertToMessage());
        }

        TopologyConfigurationMsg topologyConfig = TopologyConfigurationMsg.newBuilder()
                .setTopologyConfigID(topologyConfigId)
                .addAllClusters(clustersConfigs).build();

        return topologyConfig;
    }

    public void addStandbyCluster(ClusterDescriptor cluster) {
        standbyClusters.put(cluster.getClusterId(), cluster);
    }

    public void removeStandbyCluster(String clusterId) {
        standbyClusters.remove(clusterId);
    }

    /**
     * Retrieve Cluster Descriptor to which a given endpoint belongs to.
     *
     * @param endpoint
     * @return cluster descriptor to which endpoint belongs to.
     */
    public ClusterDescriptor getClusterDescriptor(String endpoint) {
        List<ClusterDescriptor> clusters = new ArrayList<>();
        clusters.add(activeCluster);
        clusters.addAll(standbyClusters.values());

        for(ClusterDescriptor cluster : clusters) {
            for (NodeDescriptor node : cluster.getNodesDescriptors()) {
                if (node.getEndpoint().equals(endpoint)) {
                    return cluster;
                }
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return String.format("Topology[%s] :: Active Cluster=%s :: Standby Clusters=%s", topologyConfigId, activeCluster, standbyClusters);
    }
}