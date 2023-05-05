package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryServiceAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.ExampleSchemas.ClusterUuidMsg;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Address;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;


/**
 * This class extends CorfuReplicationClusterManagerAdapter, provides topology config API
 * for integration tests. The initial topology config should be valid, which means it has only
 * one source cluster, and one or more sink clusters.
 */
@Slf4j
public class DefaultClusterManager implements CorfuReplicationClusterManagerAdapter {
    private static final int BACKUP_CORFU_PORT = 9007;

    public static final String CONFIG_NAMESPACE = "ns_lr_config_it";
    public static final String CONFIG_TABLE_NAME = "lr_config_it";
    public static final ClusterUuidMsg OP_RESUME = ClusterUuidMsg.newBuilder().setLsb(0L).setMsb(0L).build();
    public static final ClusterUuidMsg OP_SWITCH = ClusterUuidMsg.newBuilder().setLsb(1L).setMsb(1L).build();
    public static final ClusterUuidMsg OP_TWO_SOURCE = ClusterUuidMsg.newBuilder().setLsb(2L).setMsb(2L).build();
    public static final ClusterUuidMsg OP_ALL_SINK = ClusterUuidMsg.newBuilder().setLsb(3L).setMsb(3L).build();
    public static final ClusterUuidMsg OP_INVALID = ClusterUuidMsg.newBuilder().setLsb(4L).setMsb(4L).build();
    public static final ClusterUuidMsg OP_ENFORCE_SNAPSHOT_FULL_SYNC = ClusterUuidMsg.newBuilder().setLsb(5L).setMsb(5L).build();
    public static final ClusterUuidMsg OP_BACKUP = ClusterUuidMsg.newBuilder().setLsb(6L).setMsb(6L).build();

    // Topology types
    public static final ClusterUuidMsg TP_SINGLE_SOURCE_SINK = ClusterUuidMsg.newBuilder().setLsb(7L).setMsb(7L).build();
    public static final ClusterUuidMsg TP_MULTI_SINK = ClusterUuidMsg.newBuilder().setLsb(8L).setMsb(8L).build();
    public static final ClusterUuidMsg TP_MULTI_SOURCE = ClusterUuidMsg.newBuilder().setLsb(9L).setMsb(9L).build();
    public static final ClusterUuidMsg TP_MIXED_MODEL_THREE_SINK = ClusterUuidMsg.newBuilder().setLsb(10L).setMsb(10L).build();
    public static final ClusterUuidMsg TP_SINGLE_SOURCE_SINK_REV_CONNECTION = ClusterUuidMsg.newBuilder().setLsb(11L).setMsb(11L).build();
    public static final ClusterUuidMsg TP_MULTI_SOURCE_REV_CONNECTION = ClusterUuidMsg.newBuilder().setLsb(12L).setMsb(12L).build();
    public static final ClusterUuidMsg TP_MULTI_SINK_REV_CONNECTION = ClusterUuidMsg.newBuilder().setLsb(13L).setMsb(13L).build();
    public static final ClusterUuidMsg OP_TWO_SINK_MIXED = ClusterUuidMsg.newBuilder().setLsb(14L).setMsb(14L).build();

    @Getter
    private long configId;

    @Getter
    private boolean shutdown;

    @Getter
    public ClusterManagerCallback clusterManagerCallback;

    private CorfuRuntime corfuRuntime;

    private CorfuStore corfuStore;

    private ConfigStreamListener configStreamListener;

    @Getter
    private String corfuEndpoint = "localhost:9000";

    private DefaultClusterConfig topology;

    @Getter
    public CorfuReplicationDiscoveryServiceAdapter corfuReplicationDiscoveryServiceAdapter;

    @Getter
    public TopologyDescriptor topologyConfig;

    public String localEndpoint;

    @Setter
    private String localNodeId;

    // since creating the topology and LR bootstrap is async to each other, wait until clusterManger is ready with the
    // desired topology
    private CountDownLatch waitForTopologyInit = new CountDownLatch(1);

    // used for roleChange tests only. Since we rely on clusterIds to determine the role, we now need a mutable in-memory
    // structure which tracks the current source/sinks
    private Set<String> tempPrevSourceClusterIds = new HashSet<>(DefaultClusterConfig.getSourceClusterIds());
    private Set<String> tempPrevSinkClusterIds = new HashSet<>(DefaultClusterConfig.getSinkClusterIds());

    private boolean isSinkConnectionStarter = false;

    public DefaultClusterManager() {
        topology = new DefaultClusterConfig();
    }

    @Override
    public void start() {
        configId = 0L;
        shutdown = false;
        topologyConfig = initConfig();
        clusterManagerCallback = new ClusterManagerCallback(this);
        corfuRuntime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(corfuEndpoint)
                .connect();
        corfuStore = new CorfuStore(corfuRuntime);
        long trimMark = Address.NON_ADDRESS;
        try {
            // Subscribe from the earliest point in the log.
            trimMark = corfuRuntime.getLayoutView().getRuntimeLayout()
                    .getLogUnitClient(corfuRuntime.getLayoutServers().get(0)).getTrimMark().get();
        } catch (ExecutionException | InterruptedException e) {
            log.error("Exception caught while attempting to fetch trim mark. Subscription might fail.", e);
        }
        CorfuStoreMetadata.Timestamp ts = CorfuStoreMetadata.Timestamp.newBuilder()
                .setEpoch(corfuRuntime.getLayoutView().getRuntimeLayout().getLayout().getEpoch())
                .setSequence(trimMark).build();
        try {
            Table<ClusterUuidMsg, ClusterUuidMsg, ClusterUuidMsg> table = corfuStore.openTable(
                    CONFIG_NAMESPACE, CONFIG_TABLE_NAME,
                    ClusterUuidMsg.class, ClusterUuidMsg.class, ClusterUuidMsg.class,
                    TableOptions.fromProtoSchema(ClusterUuidMsg.class)
            );
            table.clearAll();
        } catch (Exception e) {
            log.error("Exception caught while opening {} table", CONFIG_TABLE_NAME);
            throw new RuntimeException(e);
        }
        configStreamListener = new ConfigStreamListener(this);
        corfuStore.subscribeListener(configStreamListener, CONFIG_NAMESPACE, "cluster_manager_test", ts);
        Thread thread = new Thread(clusterManagerCallback);
        thread.start();
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
        localNodeId = corfuReplicationDiscoveryServiceAdapter.getLocalNodeId();
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
    public void shutdown() {
        shutdown = true;
        if (corfuRuntime != null) {
            corfuRuntime.shutdown();
        }

        if(configStreamListener != null) {
            corfuStore.unsubscribeListener(configStreamListener);
        }

        log.info("Shutdown Cluster Manager completed.");
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
        List<String> sinkNodeNames = topology.getSourceNodeNames();
        List<String> sinkNodeHosts = topology.getSinkIpAddresses();
        List<String> sinkNodeIds = topology.getSinkNodeUuids();

        // Setup source cluster information
        for (int i = 0; i < sourceClusterIds.size(); i++) {
            List<NodeDescriptor> nodes = new ArrayList<>();

            for (int j = 0; j < sourceNodeNames.size(); j++) {
                NodeDescriptor nodeInfo = new NodeDescriptor(sourceNodeHosts.get(i), sourceLogReplicationPorts.get(i),
                        sourceClusterIds.get(i), sourceNodeIds.get(i), sourceNodeIds.get(i));
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
                NodeDescriptor nodeInfo = new NodeDescriptor(sinkNodeHosts.get(i), sinkLogReplicationPorts.get(i),
                        sinkClusterIds.get(i), sinkNodeIds.get(i), sinkNodeIds.get(i));
                nodes.add(nodeInfo);
            }
            ClusterDescriptor sinkCluster = new ClusterDescriptor(sinkClusterIds.get(i),
                    Integer.parseInt(sinkCorfuPorts.get(i)), nodes);

            sinkClustersToReplicationModel.put(sinkCluster,
                    addModel(Arrays.asList(LogReplication.ReplicationModel.FULL_TABLE)));
            allClusters.put(sinkCluster.getClusterId(), sinkCluster);
        }
        log.info("new topology has clusters: source: {} sink: {} connectionEndpoints: {}",
                remoteSourceToReplicationModels, remoteSinkToReplicationModels, connectionEndPoints);

        // create a backup Cluster
        List<NodeDescriptor> nodes = Arrays.asList(new NodeDescriptor(
                topology.getDefaultHost(),
                topology.getBackupLogReplicationPort(),
                DefaultClusterConfig.getBackupClusterIds().get(0),
                topology.getBackupNodesUuid().get(0),
                topology.getBackupNodesUuid().get(0)
        ));
        ClusterDescriptor backupCluster = new ClusterDescriptor(DefaultClusterConfig.getBackupClusterIds().get(0),
                BACKUP_CORFU_PORT, nodes);
        allClusters.put(backupCluster.getClusterId(), backupCluster);

        return new TopologyDescriptor(0L, sinkClustersToReplicationModel, sourceClustersToReplicationModel,
                allClusters, new HashSet<>(), localNodeId);
    }

    private void createMultiSourceSingleSinkTopology() {
        topologyConfig = initConfig();

    @Override
    public TopologyDescriptor queryTopologyConfig(boolean useCached){
        try {
            log.info("Wait until a topology is created");
            waitForTopologyInit.await();
        } catch (InterruptedException e) {
            log.error("Interrupted");
        }
        log.debug("new cluster config msg " + topologyConfig);
        return topologyConfig;
    }

    private void initSingleSourceSinkTopology() {
        topologyConfig = generateSingleSourceSinkTopolgy();

        waitForTopologyInit.countDown();
    }

    public TopologyDescriptor generateSingleSourceSinkTopolgy() {
        topologyConfig = initConfig();

        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();

        ClusterDescriptor localCluster = findLocalCluster();

        if(DefaultClusterConfig.getSourceClusterIds().contains(localCluster.getClusterId())) {
            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                    .filter(cluster -> cluster.getClusterId()
                            .equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                    .findFirst().get(), addModel(Arrays.asList(LogReplication.ReplicationModel.FULL_TABLE)));

            if(!isSinkConnectionStarter) {
                connectionEndPoints.add(topologyConfig.getRemoteSinkClusters().values().stream()
                        .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                        .findFirst().get());
            }
        } else {
            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId()
                                    .equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Arrays.asList(LogReplication.ReplicationModel.FULL_TABLE)));

            if(isSinkConnectionStarter) {
                connectionEndPoints.add(topologyConfig.getRemoteSourceClusters().values().stream()
                        .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                        .findFirst().get());
            }
        }
        log.info("new topology has clusters: source: {} sink: {} connectionEndpoints: {}",
                remoteSourceToReplicationModels, remoteSinkToReplicationModels, connectionEndPoints);

        return new TopologyDescriptor(++configId, remoteSinkToReplicationModels, remoteSourceToReplicationModels,
                topologyConfig.getAllClustersInTopology(), connectionEndPoints, localNodeId);
    }
    private void createThreeSinkMixedModelTopology() {
        topologyConfig = initConfig();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();

        ClusterDescriptor localCluster = findLocalCluster();
        if (DefaultClusterConfig.getSourceClusterIds().contains(localCluster.getClusterId())) {
            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.FULL_TABLE)));
            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(1)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.LOGICAL_GROUPS)));
            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(2)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.LOGICAL_GROUPS)));
            if(!isSinkConnectionStarter) {
                connectionEndPoints.addAll(topologyConfig.getRemoteSinkClusters().values());
            }
        } else if (localCluster.getClusterId() == DefaultClusterConfig.getSinkClusterIds().get(0)) {
            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.FULL_TABLE)));
            if(isSinkConnectionStarter) {
                connectionEndPoints.addAll(remoteSourceToReplicationModels.keySet());
            }

        } else if (localCluster.getClusterId() == DefaultClusterConfig.getSinkClusterIds().get(1)) {
            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.LOGICAL_GROUPS)));
            if(isSinkConnectionStarter) {
                connectionEndPoints.addAll(remoteSourceToReplicationModels.keySet());
            }
        } else if (localCluster.getClusterId() == DefaultClusterConfig.getSinkClusterIds().get(2)) {

            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.LOGICAL_GROUPS)));
            if(isSinkConnectionStarter) {
                connectionEndPoints.addAll(remoteSourceToReplicationModels.keySet());
            }
        }
        log.info("new Topology single-source-three-sink with mixed models: source: {} sink: {} connectionEndpoints: {}",
                remoteSourceToReplicationModels, remoteSinkToReplicationModels, connectionEndPoints);

        topologyConfig = new TopologyDescriptor(++configId, remoteSinkToReplicationModels, remoteSourceToReplicationModels,
                topologyConfig.getAllClustersInTopology(), connectionEndPoints, localNodeId);
        waitForTopologyInit.countDown();
    }

    private TopologyDescriptor generateTwoSinkMixedModelTopology() {
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();

        ClusterDescriptor localCluster = findLocalCluster();
        if (DefaultClusterConfig.getSourceClusterIds().contains(localCluster.getClusterId())) {
            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.FULL_TABLE)));
            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(1)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.LOGICAL_GROUPS)));
            if(!isSinkConnectionStarter) {
                connectionEndPoints.addAll(topologyConfig.getRemoteSinkClusters().values());
            }
        } else if (localCluster.getClusterId() == DefaultClusterConfig.getSinkClusterIds().get(0)) {
            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.FULL_TABLE)));
            if(isSinkConnectionStarter) {
                connectionEndPoints.addAll(remoteSourceToReplicationModels.keySet());
            }

        } else if (localCluster.getClusterId() == DefaultClusterConfig.getSinkClusterIds().get(1)) {
            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.LOGICAL_GROUPS)));
            if(isSinkConnectionStarter) {
                connectionEndPoints.addAll(remoteSourceToReplicationModels.keySet());
            }
        }
        log.info("new Topology single-source-two-sink with mixed models: source: {} sink: {} connectionEndpoints: {}",
                remoteSourceToReplicationModels, remoteSinkToReplicationModels, connectionEndPoints);

        return new TopologyDescriptor(++configId, remoteSinkToReplicationModels, remoteSourceToReplicationModels,
                topologyConfig.getAllClustersInTopology(), connectionEndPoints, localNodeId);
    }


    private void createSingleSourceMultiSinkTopology() {
        topologyConfig = initConfig();

        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();

        ClusterDescriptor localCluster = findLocalCluster();

        if(DefaultClusterConfig.getSourceClusterIds().contains(localCluster.getClusterId())) {
            remoteSinkToReplicationModels.putAll(topologyConfig.getRemoteSinkClusterToReplicationModels());
            if(!isSinkConnectionStarter) {
                topologyConfig.getRemoteSinkClusters().values().forEach(connectionEndPoints::add);
            }
        } else {
            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                    .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                    .findFirst().get(), addModel(Arrays.asList(LogReplication.ReplicationModel.FULL_TABLE)));

            if(isSinkConnectionStarter) {
                connectionEndPoints.addAll(remoteSourceToReplicationModels.keySet());
            }
        }
        log.info("new Topology single-source-multi-sink: source: {} sink: {} connectionEndpoints: {}",
                remoteSourceToReplicationModels, remoteSinkToReplicationModels, connectionEndPoints);

        topologyConfig = new TopologyDescriptor(++configId, remoteSinkToReplicationModels, remoteSourceToReplicationModels,
                topologyConfig.getAllClustersInTopology(), connectionEndPoints, localNodeId);

        waitForTopologyInit.countDown();
    }

    private ClusterDescriptor findLocalCluster() {
        return topologyConfig.getLocalClusterDescriptor();
    }

    private void createMultiSourceSingleSinkTopology() {
        topologyConfig = initConfig();

        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();

        ClusterDescriptor localCluster = findLocalCluster();

        if(DefaultClusterConfig.getSourceClusterIds().contains(localCluster.getClusterId())) {

            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                    .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                    .findFirst().get(), addModel(Arrays.asList(LogReplication.ReplicationModel.FULL_TABLE)));

            if (!isSinkConnectionStarter) {
                connectionEndPoints.add(topologyConfig.getRemoteSinkClusters().values().stream()
                        .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                        .findFirst().get());
            }

        } else {
            remoteSourceToReplicationModels.putAll(topologyConfig.getRemoteSourceClusterToReplicationModels());

            if (isSinkConnectionStarter) {
                connectionEndPoints.addAll(topologyConfig.getRemoteSourceClusters().values());
            }
        }

        log.info("new topology:: the multi-source-single-sink: source: {} sink: {} connectionEndpoints: {}",
                remoteSourceToReplicationModels, remoteSinkToReplicationModels, connectionEndPoints);

        topologyConfig = new TopologyDescriptor(++configId, remoteSinkToReplicationModels, remoteSourceToReplicationModels,
                topologyConfig.getAllClustersInTopology(), connectionEndPoints, localNodeId);

        waitForTopologyInit.countDown();
    }
    private void createThreeSinkMixedModelTopology() {
        topologyConfig = initConfig();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();

        ClusterDescriptor localCluster = findLocalCluster();
        if (DefaultClusterConfig.getSourceClusterIds().contains(localCluster.getClusterId())) {
            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.FULL_TABLE)));
            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(1)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.LOGICAL_GROUPS)));
            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(2)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.LOGICAL_GROUPS)));
            if(!isSinkConnectionStarter) {
                connectionEndPoints.addAll(topologyConfig.getRemoteSinkClusters().values());
            }
        } else if (localCluster.getClusterId() == DefaultClusterConfig.getSinkClusterIds().get(0)) {
            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.FULL_TABLE)));
            if(isSinkConnectionStarter) {
                connectionEndPoints.addAll(remoteSourceToReplicationModels.keySet());
            }

        } else if (localCluster.getClusterId() == DefaultClusterConfig.getSinkClusterIds().get(1)) {
            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.LOGICAL_GROUPS)));
            if(isSinkConnectionStarter) {
                connectionEndPoints.addAll(remoteSourceToReplicationModels.keySet());
            }
        } else if (localCluster.getClusterId() == DefaultClusterConfig.getSinkClusterIds().get(2)) {

            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.LOGICAL_GROUPS)));
            if(isSinkConnectionStarter) {
                connectionEndPoints.addAll(remoteSourceToReplicationModels.keySet());
            }
        }
        log.info("new Topology single-source-three-sink with mixed models: source: {} sink: {} connectionEndpoints: {}",
                remoteSourceToReplicationModels, remoteSinkToReplicationModels, connectionEndPoints);

        topologyConfig = new TopologyDescriptor(++configId, remoteSinkToReplicationModels, remoteSourceToReplicationModels,
                topologyConfig.getAllClustersInTopology(), connectionEndPoints, localNodeId);
        waitForTopologyInit.countDown();
    }

    private TopologyDescriptor generateTwoSinkMixedModelTopology() {
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();

        ClusterDescriptor localCluster = findLocalCluster();
        if (DefaultClusterConfig.getSourceClusterIds().contains(localCluster.getClusterId())) {
            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.FULL_TABLE)));
            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(1)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.LOGICAL_GROUPS)));
            if(!isSinkConnectionStarter) {
                connectionEndPoints.addAll(topologyConfig.getRemoteSinkClusters().values());
            }
        } else if (localCluster.getClusterId() == DefaultClusterConfig.getSinkClusterIds().get(0)) {
            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.FULL_TABLE)));
            if(isSinkConnectionStarter) {
                connectionEndPoints.addAll(remoteSourceToReplicationModels.keySet());
            }

        } else if (localCluster.getClusterId() == DefaultClusterConfig.getSinkClusterIds().get(1)) {
            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                            .findFirst().get(),
                    addModel(Collections.singletonList(LogReplication.ReplicationModel.LOGICAL_GROUPS)));
            if(isSinkConnectionStarter) {
                connectionEndPoints.addAll(remoteSourceToReplicationModels.keySet());
            }
        }
        log.info("new Topology single-source-two-sink with mixed models: source: {} sink: {} connectionEndpoints: {}",
                remoteSourceToReplicationModels, remoteSinkToReplicationModels, connectionEndPoints);

        return new TopologyDescriptor(++configId, remoteSinkToReplicationModels, remoteSourceToReplicationModels,
                topologyConfig.getAllClustersInTopology(), connectionEndPoints, localNodeId);
    }


    /**
     * Create a new topology config, which changes one of the sink as the source,
     * and source as sink. Data should flow in the reverse direction.
     **/
    public TopologyDescriptor generateConfigWithRoleSwitch() {
        TopologyDescriptor oldTopology = topologyConfig;

        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();

        ClusterDescriptor localCluster = findLocalCluster();


        //On role change, the role of oldCluster is flipped.
        //If old role is SOURCE (i.e., it is now the sink), it only has the remote SOURCEs connecting to it.
        //If old role is SINK (i.e., it is now the SOURCE), it has to connect to all the remote sinks.
        if(tempPrevSourceClusterIds.contains(localCluster.getClusterId())) {
            oldTopology.getRemoteSinkClusterToReplicationModels().forEach(remoteSourceToReplicationModels::put);

            //update the temp data structures
            tempPrevSourceClusterIds.remove(localCluster.getClusterId());
            tempPrevSinkClusterIds.add(localCluster.getClusterId());

        } else if(tempPrevSinkClusterIds.contains(localCluster.getClusterId())){
            oldTopology.getRemoteSourceClusterToReplicationModels().forEach(remoteSinkToReplicationModels::put);

            connectionEndPoints.addAll(remoteSinkToReplicationModels.keySet());

            //update the temp data structures
            tempPrevSinkClusterIds.remove(localCluster.getClusterId());
            tempPrevSourceClusterIds.add(localCluster.getClusterId());
        }

        log.debug("new topology :: role changed : source: {}, sink: {}, connectionEndPoints: {}",
                remoteSourceToReplicationModels, remoteSinkToReplicationModels, connectionEndPoints);

        return new TopologyDescriptor(++configId, remoteSinkToReplicationModels, remoteSourceToReplicationModels,
                oldTopology.getAllClustersInTopology(), connectionEndPoints, localNodeId);
    }

    /**
     * Using the current topology, create a new topology config, which marks all sink cluster as source on purpose.
     * System should drop messages between any two source clusters.
     **/
    public TopologyDescriptor generateConfigWithAllSource() {
        Map<String, ClusterDescriptor> newSourceClusters = new HashMap<>();
        topologyConfig.getAllClustersInTopology().values().forEach(cluster -> {
            if (DefaultClusterConfig.getSourceClusterIds().contains(cluster.getClusterId())) {
                newSourceClusters.put(cluster.getClusterId(), cluster);
            } else {
                newSourceClusters.put(cluster.getClusterId(), cluster);
            }
        });

        return new TopologyDescriptor(++configId, new HashMap<>(), new HashMap<>(), newSourceClusters,
                new HashSet<>(), localNodeId);
    }


    /**
     * Using the current topology, create a new topology config, which marks all cluster as sink on purpose.
     * System should not send messages in this case.
     **/
    public TopologyDescriptor generateConfigWithAllSink() {

        Map<String, ClusterDescriptor> newSinkClusters = new HashMap<>();
        topologyConfig.getAllClustersInTopology().values().forEach(cluster -> {
            if (DefaultClusterConfig.getSinkClusterIds().contains(cluster.getClusterId())) {
                newSinkClusters.put(cluster.getClusterId(), cluster);
            } else {
                newSinkClusters.put(cluster.getClusterId(), cluster);
            }
        });

        return new TopologyDescriptor(++configId, new HashMap<>(), new HashMap<>(), newSinkClusters,
                new HashSet<>(), localNodeId);
    }

    /**
     * Create a new topology config, which marks all sink cluster as invalid on purpose.
     * LR should not replicate to these clusters.
     **/
    public TopologyDescriptor generateConfigWithInvalid() {
        //Since all the sinks are marked as invalid, the remote source/sink/connectionEndPoints will all be empty as
        //(i) if local cluster is source, there are no sinks and hence no connectionEndPoints
        //(ii) if the local cluster is INVALID (previously sink), no LR against it.

        Map<String, ClusterDescriptor> newInvalidClusters = new HashMap<>();
        topologyConfig.getAllClustersInTopology().values().forEach(cluster -> {
            if (DefaultClusterConfig.getSinkClusterIds().contains(cluster.getClusterId())) {
                newInvalidClusters.put(cluster.getClusterId(), cluster);
            } else {
                newInvalidClusters.put(cluster.getClusterId(), cluster);
            }
        });

        return new TopologyDescriptor(++configId, new HashMap<>(), new HashMap<>(), newInvalidClusters,
                new HashSet<>(), localNodeId);
    }

    /**
     * Update the valid default topology config with sink cluster.
     * Add one sink cluster and Remove existing sink cluster.
     **/
    public TopologyDescriptor addAndRemoveSinkFromDefaultTopology() {
        initSingleSourceSinkTopology();
        // add sink
        TopologyDescriptor defaultTopology = initConfig();
        List<ClusterDescriptor> sinksToAdd = defaultTopology.getRemoteSinkClusters().values().stream()
                .filter(sink -> DefaultClusterConfig.getSinkClusterIds().get(0) != sink.getClusterId())
                .collect(Collectors.toList());

        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();
        Map<String, ClusterDescriptor> allCluster = new HashMap<>(topologyConfig.getAllClustersInTopology());

        sinksToAdd.forEach(cluster -> {
            remoteSinkToReplicationModels.put(cluster,
                    addModel(Arrays.asList(LogReplication.ReplicationModel.FULL_TABLE)));
            connectionEndPoints.add(cluster);
            allCluster.put(cluster.getClusterId(), cluster);
        });

        return new TopologyDescriptor(++configId, remoteSinkToReplicationModels,
                topologyConfig.getRemoteSourceClusterToReplicationModels(),
                allCluster, connectionEndPoints, localNodeId);
    }

    /**
     * Create a new topology config, which replaces the source cluster with a backup cluster.
     **/
    public TopologyDescriptor generateConfigWithBackup() {

        // from the initial config, extract the backup cluster
        ClusterDescriptor backupCluster = topologyConfig.getAllClustersInTopology().values().stream()
                .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getBackupClusterIds().get(0)))
                .findFirst().get();

        topologyConfig.getAllClustersInTopology().put(backupCluster.getClusterId(), backupCluster);


        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();

        ClusterDescriptor localCluster = findLocalCluster();

        if (localCluster.getClusterId().equals(backupCluster.getClusterId())){
            topologyConfig.getAllClustersInTopology().values().stream()
                    .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                    .forEach(cluster ->
                            remoteSinkToReplicationModels.put(cluster,
                                    addModel(Arrays.asList(LogReplication.ReplicationModel.FULL_TABLE))));

            if (!isSinkConnectionStarter) {
                connectionEndPoints.addAll(remoteSinkToReplicationModels.keySet());
            }
        } else if(DefaultClusterConfig.getSinkClusterIds().contains(localCluster.getClusterId())) {
            remoteSourceToReplicationModels.put(backupCluster,
                    addModel(Arrays.asList(LogReplication.ReplicationModel.FULL_TABLE)));

            if (isSinkConnectionStarter) {
                connectionEndPoints.addAll(remoteSourceToReplicationModels.keySet());
            }
        }

        log.info("added the backup as a SOURCE cluster: source: {} sink: {} connectionEndpoints: {}",
                remoteSourceToReplicationModels, remoteSinkToReplicationModels, connectionEndPoints);

        return new TopologyDescriptor(++configId, remoteSinkToReplicationModels, remoteSourceToReplicationModels,
                topologyConfig.getAllClustersInTopology(), connectionEndPoints, localNodeId);
    }

    private Set<LogReplication.ReplicationModel> addModel(List<LogReplication.ReplicationModel> modelList) {
        Set<LogReplication.ReplicationModel> supportedModels = new HashSet<>();
        modelList.forEach(model -> supportedModels.add(model));

        return supportedModels;
    }

    /**
     * Testing purpose to generate cluster role change.
     */
    public static class ClusterManagerCallback implements Runnable {
        private final DefaultClusterManager clusterManager;
        private final LinkedBlockingQueue<TopologyDescriptor> queue;

        public ClusterManagerCallback(DefaultClusterManager clusterManager) {
            this.clusterManager = clusterManager;
            queue = new LinkedBlockingQueue<>();
        }

        public void applyNewTopologyConfig(@NonNull TopologyDescriptor descriptor) {
            log.info("Applying a new config {}", descriptor);
            queue.add(descriptor);
        }

        @Override
        public void run() {
            while (!clusterManager.isShutdown()) {
                try {
                    TopologyDescriptor newConfig = queue.take();
                    clusterManager.updateTopologyConfig(newConfig);
                    log.warn("change the cluster config");
                } catch (InterruptedException ie) {
                    throw new UnrecoverableCorfuInterruptedError(ie);
                }
            }
        }
    }

    /**
     * Stream Listener on topology config table, which is for test only.
     * It enables ITs run as processes and communicate with the cluster manager
     * to update topology config.
     **/
    public static class ConfigStreamListener implements StreamListener {

        private final DefaultClusterManager clusterManager;

        public ConfigStreamListener(DefaultClusterManager clusterManager) {
            this.clusterManager = clusterManager;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            log.info("onNext :: entry size={}", results.getEntries());
            CorfuStreamEntry entry = results.getEntries().values()
                    .stream()
                    .findFirst()
                    .map(corfuStreamEntries -> corfuStreamEntries.get(0))
                    .orElse(null);
            if (entry != null && entry.getOperation().equals(CorfuStreamEntry.OperationType.UPDATE)) {
                log.info("onNext :: key={}, payload={}, metadata={}", entry.getKey(), entry.getPayload(), entry.getMetadata());
                if (entry.getKey().equals(OP_SWITCH)) {
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateConfigWithRoleSwitch());
                } else if (entry.getKey().equals(OP_TWO_SOURCE)) {
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateConfigWithAllSource());
                } else if (entry.getKey().equals(OP_ALL_SINK)) {
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateConfigWithAllSink());
                } else if (entry.getKey().equals(OP_INVALID)) {
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateConfigWithInvalid());
                } else if (entry.getKey().equals(OP_RESUME)) {
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateSingleSourceSinkTopolgy());
                } else if (entry.getKey().equals(OP_ENFORCE_SNAPSHOT_FULL_SYNC)) {
                    try {
                        ClusterDescriptor localCluster = clusterManager.findLocalCluster();
                        if (DefaultClusterConfig.getSinkClusterIds().contains(localCluster.getClusterId())) {
                            return;
                        }
                        // Enforce snapshot sync on the 1st sink cluster
                        TopologyDescriptor currentTopology = clusterManager.topologyConfig;
                        String sinkClusterId = currentTopology.getRemoteSinkClusters().keySet().stream()
                                .filter(clusterId -> clusterId.equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                                .findFirst().get();

                        String sourceClusterId = localCluster.getClusterId();

                        LogReplicationSession session = LogReplicationSession.newBuilder()
                                .setSinkClusterId(sinkClusterId)
                                .setSourceClusterId(sourceClusterId)
                                .setSubscriber(LogReplicationConfigManager.getDefaultSubscriber())
                                .build();
                        clusterManager.forceSnapshotSync(session);
                    } catch (LogReplicationDiscoveryServiceException e) {
                        log.warn("Caught a RuntimeException ", e);
                        String clusterId = clusterManager.topologyConfig.getLocalClusterDescriptor().getClusterId();
                        if (DefaultClusterConfig.getSourceClusterIds().contains(clusterId)) {
                            log.error("The current cluster role is SOURCE but forcedSnapshot Sync failed with an " +
                                    "exception", e);
                            throw new RuntimeException(e);
                        }
                    }
                } else if (entry.getKey().equals(OP_BACKUP)) {
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateConfigWithBackup());
                } else if (entry.getKey().equals(OP_TWO_SINK_MIXED)) {
                    clusterManager.isSinkConnectionStarter = true;
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateTwoSinkMixedModelTopology());
                } else if (entry.getKey().equals(TP_SINGLE_SOURCE_SINK)) {
                    clusterManager.initSingleSourceSinkTopology();
                } else if (entry.getKey().equals(TP_MULTI_SINK)) {
                    clusterManager.createSingleSourceMultiSinkTopology();
                } else if (entry.getKey().equals(TP_MULTI_SOURCE)) {
                    clusterManager.createMultiSourceSingleSinkTopology();
                } else if (entry.getKey().equals(TP_MIXED_MODEL_THREE_SINK)) {
                    clusterManager.isSinkConnectionStarter = true;
                    clusterManager.createThreeSinkMixedModelTopology();
                } else if (entry.getKey().equals(TP_MULTI_SINK_REV_CONNECTION)) {
                    clusterManager.isSinkConnectionStarter = true;
                    clusterManager.createSingleSourceMultiSinkTopology();
                } else if (entry.getKey().equals(TP_MULTI_SOURCE_REV_CONNECTION)) {
                    clusterManager.isSinkConnectionStarter = true;
                    clusterManager.createMultiSourceSingleSinkTopology();
                } else if (entry.getKey().equals(TP_SINGLE_SOURCE_SINK_REV_CONNECTION)) {
                    clusterManager.isSinkConnectionStarter = true;
                    clusterManager.initSingleSourceSinkTopology();
                }
                return;
            }

            log.info("onNext :: key={}, payload={}, metadata={}", entry.getKey(), entry.getPayload(), entry.getMetadata());
            if (entry.getKey().equals(OP_SWITCH)) {
                clusterManager.getClusterManagerCallback()
                        .applyNewTopologyConfig(clusterManager.generateConfigWithRoleSwitch());
            } else if (entry.getKey().equals(OP_TWO_SOURCE)) {
                clusterManager.getClusterManagerCallback()
                        .applyNewTopologyConfig(clusterManager.generateConfigWithAllSource());
            } else if (entry.getKey().equals(OP_ALL_SINK)) {
                clusterManager.getClusterManagerCallback()
                        .applyNewTopologyConfig(clusterManager.generateConfigWithAllSink());
            } else if (entry.getKey().equals(OP_INVALID)) {
                clusterManager.getClusterManagerCallback()
                        .applyNewTopologyConfig(clusterManager.generateConfigWithInvalid());
            } else if (entry.getKey().equals(OP_RESUME)) {
                clusterManager.getClusterManagerCallback()
                        .applyNewTopologyConfig(clusterManager.generateSingleSourceSinkTopolgy());
            } else if (entry.getKey().equals(OP_ENFORCE_SNAPSHOT_FULL_SYNC)) {
                try {
                    ClusterDescriptor localCluster = clusterManager.findLocalCluster();
                    if (DefaultClusterConfig.getSinkClusterIds().contains(localCluster.getClusterId())) {
                        return;
                    }
                    // Enforce snapshot sync on the 1st sink cluster
                    TopologyDescriptor currentTopology = clusterManager.topologyConfig;
                    String sinkClusterId = currentTopology.getRemoteSinkClusters().keySet().stream()
                            .filter(clusterId -> clusterId.equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                            .findFirst().get();

                    String sourceClusterId = localCluster.getClusterId();

                    LogReplicationSession session = LogReplicationSession.newBuilder()
                            .setSinkClusterId(sinkClusterId)
                            .setSourceClusterId(sourceClusterId)
                            .setSubscriber(LogReplicationConfigManager.getDefaultSubscriber())
                            .build();
                    clusterManager.forceSnapshotSync(session);
                } catch (LogReplicationDiscoveryServiceException e) {
                    log.warn("Caught a RuntimeException ", e);
                    String clusterId = clusterManager.topologyConfig.getLocalClusterDescriptor().getClusterId();
                    if (DefaultClusterConfig.getSourceClusterIds().contains(clusterId)) {
                        log.error("The current cluster role is SOURCE but forcedSnapshot Sync failed with an " +
                                "exception", e);
                        throw new RuntimeException(e);
                    }
                }
            } else if (entry.getKey().equals(OP_BACKUP)) {
                clusterManager.getClusterManagerCallback()
                        .applyNewTopologyConfig(clusterManager.generateConfigWithBackup());
            } else if (entry.getKey().equals(OP_TWO_SINK_MIXED)) {
                clusterManager.isSinkConnectionStarter = true;
                clusterManager.getClusterManagerCallback()
                        .applyNewTopologyConfig(clusterManager.generateTwoSinkMixedModelTopology());
            } else if (entry.getKey().equals(TP_SINGLE_SOURCE_SINK)) {
                clusterManager.initSingleSourceSinkTopology();
            } else if (entry.getKey().equals(TP_MULTI_SINK)) {
                clusterManager.createSingleSourceMultiSinkTopology();
            } else if (entry.getKey().equals(TP_MULTI_SOURCE)) {
                clusterManager.createMultiSourceSingleSinkTopology();
            } else if (entry.getKey().equals(TP_MIXED_MODEL_THREE_SINK)) {
                clusterManager.isSinkConnectionStarter = true;
                clusterManager.createThreeSinkMixedModelTopology();
            } else if (entry.getKey().equals(TP_MULTI_SINK_REV_CONNECTION)) {
                clusterManager.isSinkConnectionStarter = true;
                clusterManager.createSingleSourceMultiSinkTopology();
            } else if (entry.getKey().equals(TP_MULTI_SOURCE_REV_CONNECTION)) {
                clusterManager.isSinkConnectionStarter = true;
                clusterManager.createMultiSourceSingleSinkTopology();
            } else if (entry.getKey().equals(TP_SINGLE_SOURCE_SINK_REV_CONNECTION)) {
                clusterManager.isSinkConnectionStarter = true;
                clusterManager.initSingleSourceSinkTopology();
            }
    }

}