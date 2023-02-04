package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class represents a view of a Multi-Cluster Topology,
 *
 * Ideally, in a given topology, one cluster represents the source cluster (source of data)
 * while n others are sink clusters (backup's). However, because the topology info is provided by an
 * external adapter which can be specific to the use cases of the user, a topology might be initialized
 * with multiple source clusters and multiple sink clusters.
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

    /**
     * Defines the cluster to which this node belongs to.
     */
    @Getter
    private ClusterDescriptor localClusterDescriptor;

    @Getter
    private NodeDescriptor localNodeDescriptor;

    /**
     * Constructor
     *
     * @param topologyMessage   topology message
     * @param localNodeId       the identifier of this node
     */
    public TopologyDescriptor(TopologyConfigurationMsg topologyMessage, String localNodeId) {
        this.topologyConfigId = topologyMessage.getTopologyConfigID();
        this.sinkClusters = new HashMap<>();
        this.sourceClusters = new HashMap<>();
        this.invalidClusters = new HashMap<>();

        for (ClusterConfigurationMsg clusterConfig : topologyMessage.getClustersList()) {
            ClusterDescriptor cluster = new ClusterDescriptor(clusterConfig);
            if (clusterConfig.getRole() == ClusterRole.SOURCE) {
                sourceClusters.put(cluster.getClusterId(), cluster);
            } else if (clusterConfig.getRole() == ClusterRole.SINK) {
                sinkClusters.put(cluster.getClusterId(), cluster);
            } else {
                invalidClusters.put(cluster.getClusterId(), cluster);
            }
        }

        setLocalDescriptor(localNodeId);
    }

    /**
     * Constructor
     *
     * @param topologyConfigId topology configuration identifier (epoch)
     * @param sourceClusters source cluster's
     * @param sinkClusters sink cluster's
     */
    public TopologyDescriptor(long topologyConfigId, @NonNull List<ClusterDescriptor> sourceClusters,
                              @NonNull List<ClusterDescriptor> sinkClusters, String localNodeId) {
        this.topologyConfigId = topologyConfigId;
        this.sourceClusters = new HashMap<>();
        this.sinkClusters = new HashMap<>();
        this.invalidClusters = new HashMap<>();

        sourceClusters.forEach(sourceCluster -> this.sourceClusters.put(sourceCluster.getClusterId(), sourceCluster));
        sinkClusters.forEach(sinkCluster -> this.sinkClusters.put(sinkCluster.getClusterId(), sinkCluster));
        setLocalDescriptor(localNodeId);
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
                              @NonNull List<ClusterDescriptor> sinkClusters,
                              @NonNull List<ClusterDescriptor> invalidClusters, String localNodeId) {
        this(topologyConfigId, sourceClusters, sinkClusters, localNodeId);
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
     * Set the local cluster & node descriptors, given the endpoint of this node
     *
     * @param nodeId
     */
    public void setLocalDescriptor(String nodeId) {
        List<ClusterDescriptor> clusters = Stream.of(sourceClusters.values(), sinkClusters.values(),
                invalidClusters.values())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        for (ClusterDescriptor cluster : clusters) {
            for (NodeDescriptor node : cluster.getNodesDescriptors()) {
                if (node.getNodeId().equals(nodeId)) {
                    localNodeDescriptor = node;
                    localClusterDescriptor = cluster;
                    return;
                }
            }
        }
        log.warn("Node {} does not belong to any cluster defined in {}", nodeId, clusters);
    }

    @Override
    public String toString() {
        return String.format("Topology[id=%s] \n Source Cluster=%s \n Sink Clusters=%s \n Invalid Clusters=%s",
                topologyConfigId, sourceClusters.values(), sinkClusters.values(), invalidClusters.values());
    }
}