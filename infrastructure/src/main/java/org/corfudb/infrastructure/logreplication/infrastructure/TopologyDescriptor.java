package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.runtime.LogReplication;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    // Map of ClusterId -> ClusterConfigurationMsg. Contains the complete view of topology.
    @Getter
    private final Map<String, ClusterConfigurationMsg> allClusterMsgsInTopology = new HashMap<>();

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
     * @param topologyMessage  topology message
     * @param localNodeId      the identifier of this node
     * @param remoteSinkToReplicationModel  remote Sink to local cluster and corresponding replication models
     * @param remoteSourceToReplicationModel  remote Source to local cluster and corresponding replication models
     */
    public TopologyDescriptor(TopologyConfigurationMsg topologyMessage, String localNodeId,
                              Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModel,
                              Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModel) {
        this.topologyConfigId = topologyMessage.getTopologyConfigID();
        this.remoteSinkClusters = new HashMap<>();
        this.remoteSinkClusterToReplicationModels = new HashMap<>();


        remoteSinkToReplicationModel.entrySet().stream().forEach(e -> {
            ClusterDescriptor cluster = new ClusterDescriptor(e.getKey());
            remoteSinkClusterToReplicationModels.put(cluster, new HashSet<>(e.getValue()));
            remoteSinkClusters.put(cluster.getClusterId(), cluster);
        });


        this.remoteSourceClusters = new HashMap<>();
        this.remoteSourceClusterToReplicationModels = new HashMap<>();

        remoteSourceToReplicationModel.entrySet().stream().forEach(e -> {
            ClusterDescriptor cluster = new ClusterDescriptor(e.getKey());
            remoteSourceClusterToReplicationModels.put(cluster, new HashSet<>(e.getValue()));
            remoteSourceClusters.put(cluster.getClusterId(), cluster);
        });

        topologyMessage.getClustersList().stream()
                .forEach(clusterMsg -> allClusterMsgsInTopology.put(clusterMsg.getId(), clusterMsg));

        setLocalDescriptor(localNodeId);
    }

    /**
     * Constructor
     *
     * @param topologyConfigId topology configuration identifier (epoch)
     * @param remoteSourceClusters source clusters
     * @param remoteSinkClusters sink clusters
     * @param localNodeId       the identifier of this node
     */
    private TopologyDescriptor(long topologyConfigId, @NonNull List<ClusterDescriptor> remoteSourceClusters,
                              @NonNull List<ClusterDescriptor> remoteSinkClusters) {
        this.topologyConfigId = topologyConfigId;
        this.remoteSourceClusters = new HashMap<>();
        this.remoteSinkClusters = new HashMap<>();
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
    }

    /**
     * Constructor
     *
     * @param topologyConfigId topology configuration identifier (epoch)
     * @param remoteSourceClusters remote source clusters
     * @param remoteSinkClusters remote sink clusters
     * @param allClusters all clusters in topology
     */
    @VisibleForTesting
    public TopologyDescriptor(long topologyConfigId, @NonNull List<ClusterDescriptor> remoteSourceClusters,
                              @NonNull List<ClusterDescriptor> remoteSinkClusters,
                              @NonNull List<ClusterDescriptor> allClusters,  String localNodeId) {
        this(topologyConfigId, remoteSourceClusters, remoteSinkClusters);
        allClusters.forEach(cluster -> allClusterMsgsInTopology.put(cluster.getClusterId(), cluster.convertToMessage()));
        setLocalDescriptor(localNodeId);
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
     * Set the local cluster & node descriptors, given the endpoint of this node
     *
     * @param nodeId
     */
    public void setLocalDescriptor(String nodeId) {

        for (ClusterConfigurationMsg clusterMsg : allClusterMsgsInTopology.values()) {
            for (LogReplicationClusterInfo.NodeConfigurationMsg nodeMsg : clusterMsg.getNodeInfoList()) {
                if (nodeMsg.getNodeId().equals(nodeId)) {
                    localClusterDescriptor = new ClusterDescriptor(clusterMsg);
                    localNodeDescriptor =  new NodeDescriptor(nodeMsg.getAddress(), Integer.toString(nodeMsg.getPort()),
                            clusterMsg.getId(), nodeMsg.getConnectionId(), nodeMsg.getNodeId());
                    return;
                }
            }
        }
        log.warn("Node {} does not belong to any cluster defined in {}", nodeId, allClusterMsgsInTopology.values());
    }

    @Override
    public String toString() {
        // Find clusters which are neither Source nor Sink
        Set<ClusterDescriptor> otherClusters = new HashSet<>();
        allClusterMsgsInTopology.values().stream().forEach(clusterMsg -> otherClusters.add(new ClusterDescriptor(clusterMsg)));
        Set<ClusterDescriptor> sourceOrSinkClusters = Stream.of(remoteSourceClusters.values(), remoteSinkClusters.values())
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        otherClusters.removeAll(sourceOrSinkClusters);

        return String.format("Topology[id=%s] \n Source Cluster=%s \n Sink Clusters=%s \n Invalid Clusters=%s",
                topologyConfigId, remoteSourceClusters.values(), remoteSinkClusters.values(), otherClusters);
    }
}