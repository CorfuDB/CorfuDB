package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.LogReplication;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

    // TODO[V2] Shama: re-evaluate if all these structures are needed in connectionModel PR.
    //  Comment to address:: SessionManager also tracks incomingSessions and outgoingSessions just wondering if it might
    //  help to just have it hang on to the TopologyDescriptor to reduce the number of maps tracking the sessions
    //  perhaps? Do you think it can help reduce the overhead in keeping these in sync?

    // Contains remote clusters that are SOURCE to the local cluster.
    @Getter
    private final Map<String, ClusterDescriptor> sourceClusters;

    // Contains remote clusters that are SINK to the local cluster.
    @Getter
    private final Map<String, ClusterDescriptor> sinkClusters;

    // Map of remote ClusterDescriptor -> ReplicationModels. Contains clusters which will be SOURCE (w.r.t local cluster)
    // and the corresponding replication models
    @Getter
    private final Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceClusterToReplicationModels;

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
            if (clusterConfig.getRole() == ClusterRole.SOURCE) {
                sourceClusters.put(cluster.getClusterId(), cluster);
            } else if (clusterConfig.getRole() == ClusterRole.SINK) {
                addSinkCluster(cluster);
            } else {
                invalidClusters.put(cluster.getClusterId(), cluster);
            }
        }
    }

    /**
     * Constructor
     *
     * @param topologyConfigId topology configuration identifier (epoch)
     * @param sourceCluster source cluster
     * @param sinkClusters sink cluster's
     */
    public TopologyDescriptor(long topologyConfigId, @NonNull ClusterDescriptor sourceCluster,
                              @NonNull List<ClusterDescriptor> sinkClusters) {
        this(topologyConfigId, Collections.singletonList(sourceCluster), sinkClusters);
    }

    /**
     * Constructor
     *
     * @param topologyConfigId topology configuration identifier (epoch)
     * @param sourceClusters source cluster's
     * @param sinkClusters sink cluster's
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

        this.topologyConfigId = topologyConfigId;
        this.remoteSourceClusters = new HashMap<>();
        this.remoteSinkClusters = new HashMap<>();
        this.remoteSinkClusterToReplicationModels = new HashMap<>(remoteSinkToReplicationModel);
        this.remoteSourceClusterToReplicationModels = new HashMap<>(remoteSourceToReplicationModel);
        remoteClusterEndpoints.forEach(cluster -> this.remoteClusterEndpoints.put(cluster.getClusterId(), cluster));

        List<ClusterConfigurationMsg> clusterConfigurationMsgs = Stream.of(sourceClusters.values(),
                sinkClusters.values(), invalidClusters.values())
                .flatMap(Collection::stream)
                .map(ClusterDescriptor::convertToMessage)
                .collect(Collectors.toList());

        remoteSourceClusterToReplicationModels.keySet().forEach(sourceCluster ->
                this.remoteSourceClusters.put(sourceCluster.getClusterId(), sourceCluster));

    /**
     * Add a sink cluster to the current topology
     *
     * @param cluster sink cluster to add
     */
    public void addSinkCluster(ClusterDescriptor cluster) {
        sinkClusters.put(cluster.getClusterId(), cluster);
    }

    /**
     * Remove a sink cluster from the current topology
     *
     * @param clusterId unique identifier of the sink cluster to be removed from topology
     */
    public void removeSinkCluster(String clusterId) {
        ClusterDescriptor removedCluster = sinkClusters.remove(clusterId);

        if (removedCluster == null) {
            log.warn("Cluster {} never present as a SINK cluster.", clusterId);
        }
        TopologyDescriptor that = (TopologyDescriptor) o;
        return topologyConfigId == that.topologyConfigId &&
                remoteSourceClusters.equals(that.remoteSourceClusters) &&
                remoteSourceClusterToReplicationModels.equals(that.remoteSourceClusterToReplicationModels) &&
                allClustersInTopology.equals(that.allClustersInTopology) &&
                remoteClusterEndpoints.equals(that.remoteClusterEndpoints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topologyConfigId, remoteSourceClusters, remoteSourceClusterToReplicationModels,
                allClustersInTopology, remoteClusterEndpoints);
    }

    /**
     * Set the local cluster & node descriptors, given the endpoint of this node
     *
     * @param nodeId
     */
    public ClusterDescriptor getClusterDescriptor(String nodeId) {
        List<ClusterDescriptor> clusters = Stream.of(sourceClusters.values(), sinkClusters.values(),
                invalidClusters.values())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        for (ClusterDescriptor cluster : allClustersInTopology.values()) {
            for (NodeDescriptor node : cluster.nodeDescriptors) {
                if (node.getNodeId().equals(nodeId)) {
                    localClusterDescriptor = cluster;
                    localNodeDescriptor = node;
                    return;
                }
            }
        }
        log.warn("Node {} does not belong to any cluster defined in {}", nodeId, allClustersInTopology.values());
    }

    @Override
    public String toString() {
        return String.format("Topology[id=%s] \n Source Cluster=%s \n Sink Clusters=%s \n Invalid Clusters=%s",
                topologyConfigId, sourceClusters.values(), sinkClusters.values(), invalidClusters.values());
    }


}