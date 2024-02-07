package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryServiceAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.ReplicationStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


/**
 * This class extends CorfuReplicationClusterManagerAdapter, provides topology config API
 * for integration tests. The initial topology config should be valid, which means it has only
 * one source cluster, and one or more sink clusters.
 */
@Slf4j
public class DefaultCloudClusterManager extends DefaultClusterManager {

    @Getter
    private long configId;

    @Getter
    private boolean shutdown;

    private DefaultCloudClusterConfig topology;

    @Getter
    public CorfuReplicationDiscoveryServiceAdapter corfuReplicationDiscoveryServiceAdapter;

    @Getter
    public TopologyDescriptor topologyConfig;

    public String localEndpoint;

    @Setter
    private String localNodeId;

    private boolean isSinkConnectionStarter = false;

    public DefaultCloudClusterManager() {
        topology = new DefaultCloudClusterConfig();
    }

    @Override
    public void start() {
        configId = 0L;
        shutdown = false;
        topologyConfig = generateRollingUpgradeTopology();
        corfuReplicationDiscoveryServiceAdapter.updateTopology(topologyConfig);
    }

    @Override
    public synchronized void updateTopologyConfig(TopologyDescriptor newTopology) {
        if (newTopology.getTopologyConfigId() > topologyConfig.getTopologyConfigId()) {
            topologyConfig = newTopology;
            corfuReplicationDiscoveryServiceAdapter.updateTopology(topologyConfig);
        }
    }

    @Override
    public void register(CorfuReplicationDiscoveryServiceAdapter corfuReplicationDiscoveryServiceAdapter) {
        this.corfuReplicationDiscoveryServiceAdapter = corfuReplicationDiscoveryServiceAdapter;
        String localEndPoint = this.corfuReplicationDiscoveryServiceAdapter.getLocalEndpoint();
        localNodeId = topology.getDefaultNodeId(localEndPoint);
        log.debug("localNodeId {}", localNodeId);
    }

    @Override
    public Map<LogReplicationSession, ReplicationStatus> queryReplicationStatus() {
        return corfuReplicationDiscoveryServiceAdapter.queryReplicationStatus();
    }

    @Override
    public UUID forceSnapshotSync(LogReplicationSession session) throws LogReplicationDiscoveryServiceException {
        return corfuReplicationDiscoveryServiceAdapter.forceSnapshotSync(session);
    }

    @Override
    public String getLocalNodeId() {
        String localEndPoint = corfuReplicationDiscoveryServiceAdapter.getLocalEndpoint();
        localNodeId = topology.getDefaultNodeId(localEndPoint);
        log.debug("localNodeId {}", localNodeId);
        return localNodeId;
    }

    private TopologyDescriptor initConfig() {
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> sourceClustersToReplicationModel = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> sinkClustersToReplicationModel= new HashMap<>();
        Map<String, ClusterDescriptor> allClusters = new HashMap<>();

        List<String> sourceClusterIds = topology.getSourceClusterIds();
        List<String> sourceCorfuPorts = topology.getSourceCorfuPorts();
        List<String> sourceLogReplicationPorts =
                topology.getSourceLogReplicationPorts();
        List<String> sourceNodeNames = topology.getSourceNodeNames();
        List<String> sourceNodeHosts = topology.getSourceIpAddresses();
        List<String> sourceNodeIds = topology.getSourceNodeUuids();

        List<String> sinkClusterIds = topology.getSinkClusterIds();
        List<String> sinkCorfuPorts = topology.getSinkCorfuPorts();
        List<String> sinkLogReplicationPorts =
                topology.getSinkLogReplicationPorts();
        List<String> sinkNodeNames = topology.getSinkNodeNames();
        List<String> sinkNodeHosts = topology.getSinkIpAddresses();
        List<String> sinkNodeIds = topology.getSinkNodeUuids();

        // Setup source cluster information
        for (int i = 0; i < sourceClusterIds.size(); i++) {
            List<NodeDescriptor> nodes = new ArrayList<>();

            for (int j = 0; j < sourceNodeNames.size(); j++) {
                NodeDescriptor nodeInfo = new NodeDescriptor(sourceNodeHosts.get(j), sourceLogReplicationPorts.get(j),
                        sourceClusterIds.get(i), sourceNodeIds.get(j), sourceNodeIds.get(j));
                nodes.add(nodeInfo);
            }
            ClusterDescriptor sourceCluster = new ClusterDescriptor(sourceClusterIds.get(i),
                    Integer.parseInt(sourceCorfuPorts.get(i)), nodes);
            sourceClustersToReplicationModel.put(sourceCluster,
                    addModel(Arrays.asList(LogReplication.ReplicationModel.FULL_TABLE)));
            allClusters.put(sourceCluster.getClusterId(), sourceCluster);
        }

        // Setup sink cluster information
        for (int i = 0; i < sinkClusterIds.size(); i++) {
            List<NodeDescriptor> nodes = new ArrayList<>();
            for (int j = 0; j < sinkNodeNames.size(); j++) {
                NodeDescriptor nodeInfo = new NodeDescriptor(sinkNodeHosts.get(j), sinkLogReplicationPorts.get(j),
                        sinkClusterIds.get(i), sinkNodeIds.get(j), sinkNodeIds.get(j));
                nodes.add(nodeInfo);
            }
            ClusterDescriptor sinkCluster = new ClusterDescriptor(sinkClusterIds.get(i),
                    Integer.parseInt(sinkCorfuPorts.get(i)), nodes);

            sinkClustersToReplicationModel.put(sinkCluster,
                    addModel(Arrays.asList(LogReplication.ReplicationModel.FULL_TABLE)));
            allClusters.put(sinkCluster.getClusterId(), sinkCluster);
        }

        // create a backup Cluster
        List<NodeDescriptor> nodes = Arrays.asList(new NodeDescriptor(
                topology.getDefaultHost(),
                topology.getBackupLogReplicationPort(),
                DefaultCloudClusterConfig.getBackupClusterIds().get(0),
                topology.getBackupNodesUuid().get(0),
                topology.getBackupNodesUuid().get(0)
        ));
        ClusterDescriptor backupCluster = new ClusterDescriptor(DefaultCloudClusterConfig.getBackupClusterIds().get(0),
                BACKUP_CORFU_PORT, nodes);
        allClusters.put(backupCluster.getClusterId(), backupCluster);

        return new TopologyDescriptor(0L, sinkClustersToReplicationModel, sourceClustersToReplicationModel,
                allClusters, new HashSet<>(), localNodeId);
    }


    @Override
    public TopologyDescriptor queryTopologyConfig(boolean useCached){
        return topologyConfig;
    }

    public TopologyDescriptor generateRollingUpgradeTopology() {
        topologyConfig = initConfig();

        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();

        ClusterDescriptor localCluster = findLocalCluster();

        if(DefaultCloudClusterConfig.getSourceClusterIds().contains(localCluster.getClusterId())) {
            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                    .filter(cluster -> cluster.getClusterId()
                            .equals(DefaultCloudClusterConfig.getSinkClusterIds().get(0)))
                    .findFirst().get(), addModel(Arrays.asList(LogReplication.ReplicationModel.FULL_TABLE)));

            if(!isSinkConnectionStarter) {
                connectionEndPoints.add(topologyConfig.getRemoteSinkClusters().values().stream()
                        .filter(cluster -> cluster.getClusterId().equals(DefaultCloudClusterConfig.getSinkClusterIds().get(0)))
                        .findFirst().get());
            }
        } else {
            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId()
                                    .equals(DefaultCloudClusterConfig.getSourceClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Arrays.asList(LogReplication.ReplicationModel.FULL_TABLE)));

            if(isSinkConnectionStarter) {
                connectionEndPoints.add(topologyConfig.getRemoteSourceClusters().values().stream()
                        .filter(cluster -> cluster.getClusterId().equals(DefaultCloudClusterConfig.getSourceClusterIds().get(0)))
                        .findFirst().get());
            }
        }
        log.info("new topology has clusters: source: {} sink: {} connectionEndpoints: {}",
                remoteSourceToReplicationModels, remoteSinkToReplicationModels, connectionEndPoints);

        return new TopologyDescriptor(++configId, remoteSinkToReplicationModels, remoteSourceToReplicationModels,
                topologyConfig.getAllClustersInTopology(), connectionEndPoints, localNodeId);
    }

    private ClusterDescriptor findLocalCluster() {
        return topologyConfig.getLocalClusterDescriptor();
    }

    private Set<LogReplication.ReplicationModel> addModel(List<LogReplication.ReplicationModel> modelList) {
        Set<LogReplication.ReplicationModel> supportedModels = new HashSet<>();
        modelList.forEach(model -> supportedModels.add(model));

        return supportedModels;
    }
}
