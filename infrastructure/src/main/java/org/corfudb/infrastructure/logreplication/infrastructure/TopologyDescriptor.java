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
    private final Map<String, ClusterDescriptor> remoteSourceClusters;

    // Contains remote clusters that are SINK to the local cluster.
    @Getter
    private final Map<String, ClusterDescriptor> remoteSinkClusters;

    // Map of remote ClusterDescriptor -> ReplicationModels. Contains clusters which will be SOURCE (w.r.t local cluster)
    // and the corresponding replication models
    @Getter
    private final Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceClusterToReplicationModels;

    // Map of remote ClusterDescriptor -> ReplicationModels. Contains clusters which will be SINK (w.r.t local cluster)
    // and the corresponding replication models
    @Getter
    private final Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkClusterToReplicationModels;

    // Map of ClusterId -> ClusterDescriptor. Contains the complete view of topology.
    @Getter
    private final Map<String, ClusterDescriptor> allClustersInTopology = new HashMap<>();

    // Remote cluster to which the local cluster will initiate connection
    @Getter
    private final Map<String, ClusterDescriptor> remoteClusterEndpoints = new HashMap<>();

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
     * @param topologyConfigId  topology ID
     * @param localNodeId      the identifier of this node
     * @param remoteSinkToReplicationModel  remote Sink to local cluster and corresponding replication models
     * @param remoteSourceToReplicationModel  remote Source to local cluster and corresponding replication models
     * @param remoteClusterEndpoints remote clusters to which the local cluster will initiate connection
     * @param allClusters     all remote clusters present in the topology
     */
    public TopologyDescriptor(long topologyConfigId, String localNodeId,
                              Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModel,
                              Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModel,
                              Set<ClusterDescriptor> remoteClusterEndpoints, Set<ClusterDescriptor> allClusters) {

        this.topologyConfigId = topologyConfigId;
        this.remoteSinkClusterToReplicationModels = remoteSinkToReplicationModel;
        this.remoteSourceClusterToReplicationModels = remoteSourceToReplicationModel;
        remoteClusterEndpoints.forEach(cluster -> this.remoteClusterEndpoints.put(cluster.getClusterId(), cluster));

        this.remoteSinkClusters = new HashMap<>();
        remoteSinkToReplicationModel.keySet().forEach(cluster -> {
            remoteSinkClusters.put(cluster.getClusterId(), cluster);
        });


        this.remoteSourceClusters = new HashMap<>();
        remoteSourceToReplicationModel.keySet().forEach(cluster -> {
            remoteSourceClusters.put(cluster.getClusterId(), cluster);
        });

        allClusters.forEach(cluster -> {
            allClustersInTopology.put(cluster.getClusterId(), cluster);
        });

        setLocalDescriptor(localNodeId);
    }

    /**
     * Constructor
     *
     * @param topologyConfigId   topology ID
     * @param remoteSinkToReplicationModel   remote Sink to local cluster and corresponding replication models
     * @param remoteSourceToReplicationModel  remote Source to local cluster and corresponding replication models
     * @param allClusters  all clusters in topology
     * @param remoteClusterEndpoints  remote clusters to which the local cluster will initiate connection
     * @param localNodeId  the identifier of this node
     */
    @VisibleForTesting
    public TopologyDescriptor(long topologyConfigId,
                              @NonNull Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModel,
                              @NonNull Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModel,
                              @NonNull Map<String, ClusterDescriptor> allClusters,
                              Set<ClusterDescriptor> remoteClusterEndpoints,
                              String localNodeId) {

        this.topologyConfigId = topologyConfigId;
        this.remoteSourceClusters = new HashMap<>();
        this.remoteSinkClusters = new HashMap<>();
        this.remoteSinkClusterToReplicationModels = new HashMap<>(remoteSinkToReplicationModel);
        this.remoteSourceClusterToReplicationModels = new HashMap<>(remoteSourceToReplicationModel);
        remoteClusterEndpoints.forEach(cluster -> this.remoteClusterEndpoints.put(cluster.getClusterId(), cluster));

        remoteSinkClusterToReplicationModels.keySet().forEach(sinkCluster ->
                this.remoteSinkClusters.put(sinkCluster.getClusterId(), sinkCluster));

        remoteSourceClusterToReplicationModels.keySet().forEach(sourceCluster ->
                this.remoteSourceClusters.put(sourceCluster.getClusterId(), sourceCluster));

        this.allClustersInTopology.putAll(allClusters);
        setLocalDescriptor(localNodeId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
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
    public void setLocalDescriptor(String nodeId) {

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
        // Find clusters which are neither Source nor Sink for logging
        Set<ClusterDescriptor> otherClusters = new HashSet<>(allClustersInTopology.values());
        Set<ClusterDescriptor> sourceOrSinkClusters = Stream.of(remoteSourceClusters.values(), remoteSinkClusters.values())
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        otherClusters.removeAll(sourceOrSinkClusters);

        return String.format("Topology[id=%s] \n Remote Source Cluster=%s \n Remote Sink Clusters=%s \n connection endpoints=%s \nOther Clusters=%s",
                topologyConfigId, remoteSourceClusters.values(), remoteSinkClusters.values(),remoteClusterEndpoints.values(), otherClusters);
    }


}