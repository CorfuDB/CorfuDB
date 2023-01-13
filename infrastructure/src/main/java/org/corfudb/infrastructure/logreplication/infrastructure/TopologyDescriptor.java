package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.CorfuReplicationClusterManagerAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.runtime.LogReplication;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    // contains remote clusters that's a SOURCE to the local cluster. Used while creating LR components and connection init
    @Getter
    private final Map<String, ClusterDescriptor> remoteSourceClusters;

    // contains remote clusters that's a SINK to local cluster. Used while creating LR components and connection init
    @Getter
    private final Map<String, ClusterDescriptor> remoteSinkClusters;

    //Map of remote clusterId -> ReplicationModels. Contains clusters which will be SOURCE (w.r.t local cluster) for a
    // replication model. Used to construct sessions.
    @Getter
    private final Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceClusterToReplicationModels;

    //Map of remote clusterId -> ReplicationModels. Contains clusters which will be SOURCE (w.r.t local cluster) for a
    // replication model. Used to construct sessions.
    @Getter
    private final Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkClusterToReplicationModels;

    @Getter
    private final Map<String, ClusterDescriptor> invalidClusters;

    // Contains the complete view of topology.
    @Getter
    private final Map<String, ClusterConfigurationMsg> allClusterMsgsInTopology = new HashMap<>();


    /**
     * Constructor used by discoveryService and tests
     *
     * @param topologyMessage proto definition of the topology
     * @param clusterManagerAdapter the plugin to fetch the cluster information
     */
    public TopologyDescriptor(TopologyConfigurationMsg topologyMessage, CorfuReplicationClusterManagerAdapter clusterManagerAdapter) {
        this(topologyMessage.getTopologyConfigID(), clusterManagerAdapter);
        topologyMessage.getClustersList().stream()
                .forEach(clusterMsg -> allClusterMsgsInTopology.put(clusterMsg.getId(), clusterMsg));
    }

    /**
     * Constructor
     *
     * @param topologyConfigId topology configuration identifier (epoch)
     * @param clusterManagerAdapter the plugin to fetch the cluster information
     */
    private TopologyDescriptor(long topologyConfigId, CorfuReplicationClusterManagerAdapter clusterManagerAdapter) {
        this.topologyConfigId = topologyConfigId;
        this.remoteSinkClusters = new HashMap<>();
        this.remoteSinkClusterToReplicationModels = new HashMap<>();

        Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModel =
                clusterManagerAdapter.getRemoteSinkToReplicationModels();
        remoteSinkToReplicationModel.entrySet().stream().forEach(e -> {
            ClusterDescriptor cluster = new ClusterDescriptor(e.getKey());
            remoteSinkClusterToReplicationModels.putIfAbsent(cluster, new HashSet<>());
            e.getValue().forEach(model -> remoteSinkClusterToReplicationModels.get(cluster).add(model));
            remoteSinkClusters.put(cluster.getClusterId(), cluster);
        });


        this.remoteSourceClusters = new HashMap<>();
        this.remoteSourceClusterToReplicationModels = new HashMap<>();
        Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModel =
                clusterManagerAdapter.getRemoteSourceToReplicationModels();
        remoteSourceToReplicationModel.entrySet().stream().forEach(e -> {
            ClusterDescriptor cluster = new ClusterDescriptor(e.getKey());
            remoteSourceClusterToReplicationModels.putIfAbsent(cluster, new HashSet<>());
            e.getValue().forEach(model -> remoteSourceClusterToReplicationModels.get(cluster).add(model));
            remoteSourceClusters.put(cluster.getClusterId(), cluster);
        });
        this.invalidClusters = new HashMap<>();
    }


    /**
     * Constructor used by tests
     *
     * @param topologyConfigId topology configuration identifier (epoch)
     * @param remoteSourceClusters source clusters to the local cluster
     * @param remoteSinkClusters sink clusters to the local cluster
     */
    public TopologyDescriptor(long topologyConfigId, @NonNull List<ClusterDescriptor> remoteSourceClusters,
                              @NonNull List<ClusterDescriptor> remoteSinkClusters) {
        this.topologyConfigId = topologyConfigId;
        this.remoteSourceClusters = new HashMap<>();
        this.remoteSinkClusters = new HashMap<>();
        this.invalidClusters = new HashMap<>();
        this.remoteSourceClusterToReplicationModels = new HashMap<>();
        this.remoteSinkClusterToReplicationModels = new HashMap<>();

        remoteSourceClusters.forEach(sourceCluster -> {
            this.remoteSourceClusters.put(sourceCluster.getClusterId(), sourceCluster);
            this.remoteSourceClusterToReplicationModels.putIfAbsent(sourceCluster, new HashSet<>());
            this.remoteSourceClusterToReplicationModels.get(sourceCluster).add(LogReplication.ReplicationModel.FULL_TABLE);
        });

        remoteSinkClusters.forEach(sinkCluster -> {
            this.remoteSinkClusters.put(sinkCluster.getClusterId(), sinkCluster);
            this.remoteSinkClusterToReplicationModels.putIfAbsent(sinkCluster, new HashSet<>());
            this.remoteSinkClusterToReplicationModels.get(sinkCluster).add(LogReplication.ReplicationModel.FULL_TABLE);
        });

        remoteSourceClusters.stream()
                .forEach(clusterDescriptor ->
                        allClusterMsgsInTopology.put(clusterDescriptor.getClusterId(), clusterDescriptor.convertToMessage()));

        remoteSinkClusters.stream()
                .forEach(clusterDescriptor ->
                        allClusterMsgsInTopology.put(clusterDescriptor.getClusterId(), clusterDescriptor.convertToMessage()));
    }

    /**
     * Constructor used by tests
     *
     * @param topologyConfigId topology configuration identifier (epoch)
     * @param remoteSourceClusters remote source clusters
     * @param remoteSinkClusters remote sink clusters
     * @param invalidClusters invalid clusters
     */
    public TopologyDescriptor(long topologyConfigId, @NonNull List<ClusterDescriptor> remoteSourceClusters,
                              @NonNull List<ClusterDescriptor> remoteSinkClusters, @NonNull List<ClusterDescriptor> invalidClusters) {
        this(topologyConfigId, remoteSourceClusters, remoteSinkClusters);
        invalidClusters.forEach(invalidCluster -> {
            this.invalidClusters.put(invalidCluster.getClusterId(), invalidCluster);
            allClusterMsgsInTopology.put(invalidCluster.getClusterId(), invalidCluster.convertToMessage());
        });
    }

    /**
     * Convert Topology Descriptor to ProtoBuf Definition
     *
     * @return topology protoBuf
     */
    public TopologyConfigurationMsg convertToMessage() {

        return TopologyConfigurationMsg.newBuilder()
                .setTopologyConfigID(topologyConfigId)
                .addAllClusters(allClusterMsgsInTopology.values()).build();
    }

    /**
     * Get the Cluster Descriptor to which a given endpoint belongs to.
     *
     * @param nodeId
     * @return cluster descriptor to which endpoint belongs to.
     */
    public ClusterDescriptor getClusterDescriptor(String nodeId) {

        for(ClusterConfigurationMsg clusterConfigMsg : allClusterMsgsInTopology.values()) {
            for (LogReplicationClusterInfo.NodeConfigurationMsg nodeMsg : clusterConfigMsg.getNodeInfoList()) {
                if (nodeMsg.getNodeId().equals(nodeId)) {
                    return new ClusterDescriptor(clusterConfigMsg);
                }
            }
        }
        log.warn("Node {} does not belong to any cluster defined in {}", nodeId, allClusterMsgsInTopology.values());

        return null;
    }

    @Override
    public String toString() {
        return String.format("Topology[id=%s] \n Source Cluster=%s \n Sink Clusters=%s \n Invalid Clusters=%s",
                topologyConfigId, remoteSourceClusters.values(), remoteSinkClusters.values(), invalidClusters.values());
    }
}