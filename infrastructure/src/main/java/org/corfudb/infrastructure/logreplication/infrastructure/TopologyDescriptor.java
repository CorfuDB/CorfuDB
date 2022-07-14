package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class represents a view of a Multi-Cluster Topology,
 *
 * A topology may have multiple source clusters and multiple sink clusters.
 *
 */
@Slf4j
public class TopologyDescriptor {

    // Represents a state of the topology configuration (a topology epoch)
    @Getter
    private final long topologyConfigId;

    @Getter
    private final Map<String, ClusterDescriptor> sourceClusters;

    @Getter
    private final Map<String, ClusterDescriptor> sinkClusters;

    @Getter
    private final Map<String, ClusterDescriptor> invalidClusters;

    // Shama: change this, the source/sink clusters should not be formed by fetch, but can be moved to bootstrap in discoverService
    /**
     * Constructor
     *
     * @param topologyMessage proto definition of the topology
     */
    public TopologyDescriptor(TopologyConfigurationMsg topologyMessage) {
        this.topologyConfigId = topologyMessage.getTopologyConfigID();
        this.sinkClusters = new HashMap<>();
        this.sourceClusters = new HashMap<>();
        this.invalidClusters = new HashMap<>();

        for (ClusterConfigurationMsg clusterConfig : topologyMessage.getClustersList()) {
            ClusterDescriptor cluster = new ClusterDescriptor(clusterConfig);
            if (clusterConfig.getRole() == ClusterRole.INVALID) {
                invalidClusters.put(cluster.getClusterId(), cluster);
            }
        }
    }


    /**
     * for test, Constructor
     *
     * @param topologyConfigId topology configuration identifier (epoch)
     * @param sourceClusters active clusters
     * @param sinkClusters sink clusters
     */
    public TopologyDescriptor(long topologyConfigId, @NonNull List<ClusterDescriptor> sourceClusters,
                              @NonNull List<ClusterDescriptor> sinkClusters) {
        this.topologyConfigId = topologyConfigId;
        this.sourceClusters = new HashMap<>();
        this.sinkClusters = new HashMap<>();
        this.invalidClusters = new HashMap<>();

        sourceClusters.forEach(sourceCluster -> this.sourceClusters.put(sourceCluster.getClusterId(), sourceCluster));
        sinkClusters.forEach(sinkCluster -> this.sinkClusters.put(sinkCluster.getClusterId(), sinkCluster));
    }

    public void setSinkClusters(Set<ClusterDescriptor> sinkClustersSet) {
        sinkClustersSet.stream().forEach(sinkCluster -> sinkClusters.put(sinkCluster.getClusterId(), sinkCluster));
    }

    public void setSourceClusters(ClusterDescriptor localCluster) {
        sourceClusters.put(localCluster.getClusterId(), localCluster);
    }

    /**
     * Constructor
     *
     * @param topologyConfigId topology configuration identifier (epoch)
     * @param sourceClusters source cluster's
     * @param sinkClusters sink cluster's
     * @param invalidClusters invalid cluster's
     */
    public TopologyDescriptor(long topologyConfigId, @NonNull List<ClusterDescriptor> sourceClusters,
                              @NonNull List<ClusterDescriptor> sinkClusters, @NonNull List<ClusterDescriptor> invalidClusters) {
        this(topologyConfigId, sourceClusters, sinkClusters);
        invalidClusters.forEach(invalidCluster -> this.invalidClusters.put(invalidCluster.getClusterId(), invalidCluster));
    }

    /**
     * Convert Topology Descriptor to ProtoBuf Definition
     *
     * @return topology protoBuf
     */
    public TopologyConfigurationMsg convertToMessage() {

        List<ClusterConfigurationMsg> clusterConfigurationMsgs = Stream.of(sourceClusters.values(),
                sinkClusters.values(), invalidClusters.values())
                .flatMap(Collection::stream)
                .map(ClusterDescriptor::convertToMessage)
                .collect(Collectors.toList());

        return TopologyConfigurationMsg.newBuilder()
                .setTopologyConfigID(topologyConfigId)
                .addAllClusters(clusterConfigurationMsgs).build();
    }

    /**
     * Add a sink cluster to the current topology
     *
     * @param cluster sink cluster to add
     */
    public void addStandbyCluster(ClusterDescriptor cluster) {
        sinkClusters.put(cluster.getClusterId(), cluster);
    }

    /**
     * Remove a standby cluster from the current topology
     *
     * @param clusterId unique identifier of the standby cluster to be removed from topology
     */
    public void removeStandbyCluster(String clusterId) {
        ClusterDescriptor removedCluster = sinkClusters.remove(clusterId);

        if (removedCluster == null) {
            log.warn("Cluster {} never present as a STANDBY cluster.", clusterId);
        }
    }

    /**
     * Get the Cluster Descriptor to which a given endpoint belongs to.
     *
     * @param nodeId
     * @return cluster descriptor to which endpoint belongs to.
     */
    public ClusterDescriptor getClusterDescriptor(String nodeId) {
        List<ClusterDescriptor> clusters = Stream.of(sourceClusters.values(), sinkClusters.values(),
                invalidClusters.values())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        for(ClusterDescriptor cluster : clusters) {
            for (NodeDescriptor node : cluster.getNodesDescriptors()) {
                if (node.getNodeId().equals(nodeId)) {
                    return cluster;
                }
            }
        }
        log.warn("Node {} does not belong to any cluster defined in {}", nodeId, clusters);

        return null;
    }

    @Override
    public String toString() {
        return String.format("Topology[id=%s] \n Active Cluster=%s \n Standby Clusters=%s \n Invalid Clusters=%s",
                topologyConfigId, sourceClusters.values(), sinkClusters.values(), invalidClusters.values());
    }
}