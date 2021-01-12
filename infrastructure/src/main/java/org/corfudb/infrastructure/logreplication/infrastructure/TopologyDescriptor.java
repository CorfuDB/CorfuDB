package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class represents a view of a Multi-Cluster Topology,
 *
 * Ideally, in a given topology, one cluster represents the active cluster (source of data)
 * while n others are standby clusters (backup's). However, because the topology info is provided by an
 * external adapter which can be specific to the use cases of the user, a topology might be initialized
 * with multiple active clusters and multiple standby clusters.
 *
 */
@Slf4j
public class TopologyDescriptor {

    // Represents a state of the topology configuration (a topology epoch)
    @Getter
    private final long topologyConfigId;

    @Getter
    private final Map<String, ClusterDescriptor> activeClusters;

    @Getter
    private final Map<String, ClusterDescriptor> standbyClusters;

    @Getter
    private final Map<String, ClusterDescriptor> invalidClusters;

    /**
     * Constructor
     *
     * @param topologyMessage proto definition of the topology
     */
    public TopologyDescriptor(TopologyConfigurationMsg topologyMessage) {
        this.topologyConfigId = topologyMessage.getTopologyConfigID();
        this.standbyClusters = new HashMap<>();
        this.activeClusters = new HashMap<>();
        this.invalidClusters = new HashMap<>();

        for (ClusterConfigurationMsg clusterConfig : topologyMessage.getClustersList()) {
            ClusterDescriptor cluster = new ClusterDescriptor(clusterConfig);
            if (clusterConfig.getRole() == ClusterRole.ACTIVE) {
                activeClusters.put(cluster.getClusterId(), cluster);
            } else if (clusterConfig.getRole() == ClusterRole.STANDBY) {
                addStandbyCluster(cluster);
            } else {
                invalidClusters.put(cluster.getClusterId(), cluster);
            }
        }
    }

    /**
     * Constructor
     *
     * @param topologyConfigId topology configuration identifier (epoch)
     * @param activeCluster active cluster
     * @param standbyClusters standby cluster's
     */
    public TopologyDescriptor(long topologyConfigId, @NonNull ClusterDescriptor activeCluster,
                              @NonNull List<ClusterDescriptor> standbyClusters) {
        this(topologyConfigId, Collections.singletonList(activeCluster), standbyClusters);
    }

    /**
     * Constructor
     *
     * @param topologyConfigId topology configuration identifier (epoch)
     * @param activeClusters active cluster's
     * @param standbyClusters standby cluster's
     */
    public TopologyDescriptor(long topologyConfigId, @NonNull List<ClusterDescriptor> activeClusters,
                              @NonNull List<ClusterDescriptor> standbyClusters) {
        this.topologyConfigId = topologyConfigId;
        this.activeClusters = new HashMap<>();
        this.standbyClusters = new HashMap<>();
        this.invalidClusters = new HashMap<>();

        activeClusters.forEach(activeCluster -> this.activeClusters.put(activeCluster.getClusterId(), activeCluster));
        standbyClusters.forEach(standbyCluster -> this.standbyClusters.put(standbyCluster.getClusterId(), standbyCluster));
    }

    /**
     * Constructor
     *
     * @param topologyConfigId topology configuration identifier (epoch)
     * @param activeClusters active cluster's
     * @param standbyClusters standby cluster's
     * @param invalidClusters invalid cluster's
     */
    public TopologyDescriptor(long topologyConfigId, @NonNull List<ClusterDescriptor> activeClusters,
                              @NonNull List<ClusterDescriptor> standbyClusters, @NonNull List<ClusterDescriptor> invalidClusters) {
        this(topologyConfigId, activeClusters, standbyClusters);
        invalidClusters.forEach(invalidCluster -> this.invalidClusters.put(invalidCluster.getClusterId(), invalidCluster));
    }

    /**
     * Convert Topology Descriptor to ProtoBuf Definition
     *
     * @return topology protoBuf
     */
    public TopologyConfigurationMsg convertToMessage() {

        List<ClusterConfigurationMsg> clusterConfigurationMsgs = Stream.of(activeClusters.values(),
                standbyClusters.values(), invalidClusters.values())
                .flatMap(Collection::stream)
                .map(ClusterDescriptor::convertToMessage)
                .collect(Collectors.toList());

        return TopologyConfigurationMsg.newBuilder()
                .setTopologyConfigID(topologyConfigId)
                .addAllClusters(clusterConfigurationMsgs).build();
    }

    /**
     * Add a standby cluster to the current topology
     *
     * @param cluster standby cluster to add
     */
    public void addStandbyCluster(ClusterDescriptor cluster) {
        standbyClusters.put(cluster.getClusterId(), cluster);
    }

    /**
     * Remove a standby cluster from the current topology
     *
     * @param clusterId unique identifier of the standby cluster to be removed from topology
     */
    public void removeStandbyCluster(String clusterId) {
        ClusterDescriptor removedCluster = standbyClusters.remove(clusterId);

        if (removedCluster == null) {
            log.warn("Cluster {} never present as a STANDBY cluster.", clusterId);
        }
    }

    /**
     * Get the Cluster Descriptor to which a given endpoint belongs to.
     *
     * @param endpoint
     * @param nodeId
     * @return cluster descriptor to which endpoint belongs to.
     */
    public ClusterDescriptor getClusterDescriptor(String endpoint, Optional<String> nodeId) {
        List<ClusterDescriptor> clusters = Stream.of(activeClusters.values(), standbyClusters.values(),
                invalidClusters.values())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        if (!nodeId.isPresent()) {
            for(ClusterDescriptor cluster : clusters) {
                for (NodeDescriptor node : cluster.getNodesDescriptors()) {
                    if (node.getEndpoint().equals(endpoint)) {
                        return cluster;
                    }
                }
            }
            log.warn("Endpoint {} does not belong to any cluster defined in {}", endpoint, clusters);
        } else {
            for(ClusterDescriptor cluster : clusters) {
                for (NodeDescriptor node : cluster.getNodesDescriptors()) {
                    if (node.getRealNodeId().toString().equals(nodeId.get())) {
                        return cluster;
                    }
                }
            }
            log.warn("Node {} does not belong to any cluster defined in {}", nodeId.get(), clusters);
        }

        return null;
    }

    @Override
    public String toString() {
        return String.format("Topology[id=%s] :: Active Cluster=%s :: Standby Clusters=%s :: Invalid Clusters=%s",
                topologyConfigId, activeClusters, standbyClusters, invalidClusters);
    }
}