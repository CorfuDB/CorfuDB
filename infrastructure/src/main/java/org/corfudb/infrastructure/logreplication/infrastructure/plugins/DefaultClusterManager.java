package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor.ClusterRole;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.ExampleSchemas.ClusterUuidMsg;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Address;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

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
    public CorfuReplicationDiscoveryService corfuReplicationDiscoveryService;

    @Getter
    public TopologyDescriptor topologyConfig;

    public String localEndpoint;

    @Setter
    private String localNodeId;

    private CountDownLatch waitForTopologyInit = new CountDownLatch(1);

    public DefaultClusterManager() {
        topology = new DefaultClusterConfig();
    }

    @Override
    public void setLocalEndpoint(String endpoint) {
        this.localEndpoint = endpoint;
    }

    @Override
    public synchronized void updateTopologyConfig(TopologyDescriptor newTopology) {
        if (newTopology.getTopologyConfigId() > topologyConfig.getTopologyConfigId()) {
            topologyConfig = newTopology;
            corfuReplicationDiscoveryService.updateTopology(topologyConfig);
        }
    }

    @Override
    public void register(CorfuReplicationDiscoveryService corfuReplicationDiscoveryService) {
        this.corfuReplicationDiscoveryService = corfuReplicationDiscoveryService;
        localNodeId = corfuReplicationDiscoveryService.getLocalNodeId();
        log.debug("localNodeId {}", localNodeId);
    }

    public void start() {
        configId = 0L;
        shutdown = false;
        topologyConfig = readConfig();
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
    public Map<LogReplicationSession, LogReplicationMetadata.ReplicationStatus> queryReplicationStatus() {
        return corfuReplicationDiscoveryService.queryReplicationStatus();
    }

    @Override
    public UUID forceSnapshotSync(LogReplicationSession session) throws LogReplicationDiscoveryServiceException {
        return corfuReplicationDiscoveryService.forceSnapshotSync(session);
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

    public TopologyDescriptor readConfig() {
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
            ClusterDescriptor sourceCluster = new ClusterDescriptor(sourceClusterIds.get(i), ClusterRole.SOURCE,
                Integer.parseInt(sourceCorfuPorts.get(i)));

            for (int j = 0; j < sourceNodeNames.size(); j++) {
                NodeDescriptor nodeInfo = new NodeDescriptor(sourceNodeHosts.get(i), sourceLogReplicationPorts.get(i),
                        sourceClusterIds.get(i), sourceNodeIds.get(i), sourceNodeIds.get(i));
                sourceCluster.getNodesDescriptors().add(nodeInfo);
            }
            sourceClustersToReplicationModel.put(sourceCluster,
                    new HashSet<LogReplication.ReplicationModel>(){
                {
                    add(LogReplication.ReplicationModel.FULL_TABLE);
                }});
            allClusters.put(sourceCluster.getClusterId(), sourceCluster);
        }

        // Setup sink cluster information
        for (int i = 0; i < sinkClusterIds.size(); i++) {
            ClusterDescriptor sinkCluster = new ClusterDescriptor(sinkClusterIds.get(i), ClusterRole.SINK,
                    Integer.parseInt(sinkCorfuPorts.get(i)));

            for (int j = 0; j < sinkNodeNames.size(); j++) {
                NodeDescriptor nodeInfo = new NodeDescriptor(sinkNodeHosts.get(i), sinkLogReplicationPorts.get(i),
                        sinkClusterIds.get(i), sinkNodeIds.get(i), sinkNodeIds.get(i));
                sinkCluster.getNodesDescriptors().add(nodeInfo);
            }
            sinkClustersToReplicationModel.put(sinkCluster,
                    new HashSet<LogReplication.ReplicationModel>(){
                        {
                            add(LogReplication.ReplicationModel.FULL_TABLE);
                        }});
            allClusters.put(sinkCluster.getClusterId(), sinkCluster);
        }

        return new TopologyDescriptor(0L, sinkClustersToReplicationModel, sourceClustersToReplicationModel,
                allClusters, new HashSet<>(), localNodeId);
    }


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

    private void createSingleSourceSinkTopologyBuckets() {
        topologyConfig = readConfig();

        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();

        ClusterDescriptor localCluster = findLocalCluster();

        if(localCluster.getRole().equals(ClusterRole.SOURCE)) {
            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId()
                                    .equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                            .findFirst().get(),
                    new HashSet<LogReplication.ReplicationModel>(){{ add(LogReplication.ReplicationModel.FULL_TABLE);
            }});
            connectionEndPoints.add(topologyConfig.getRemoteSinkClusters().values().stream()
                    .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                    .findFirst().get());
        } else {
            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                            .filter(cluster -> cluster.getClusterId()
                                    .equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                            .findFirst().get(),
                    new HashSet<LogReplication.ReplicationModel>(){{ add(LogReplication.ReplicationModel.FULL_TABLE);
                    }});
        }
        log.info("new topology has clusters: source: {} sink: {} connectionEndpoints: {}",
                remoteSourceToReplicationModels, remoteSinkToReplicationModels, connectionEndPoints);

        topologyConfig = new TopologyDescriptor(++configId, remoteSinkToReplicationModels, remoteSourceToReplicationModels,
                topologyConfig.getAllClustersInTopology(), connectionEndPoints, localNodeId);

        waitForTopologyInit.countDown();
    }

    private void createSingleSourceMultiSinkTopologyBuckets() {
        topologyConfig = readConfig();

        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();

        ClusterDescriptor localCluster = findLocalCluster();

        if(localCluster.getRole().equals(ClusterRole.SOURCE)) {
            remoteSinkToReplicationModels.putAll(topologyConfig.getRemoteSinkClusterToReplicationModels());
            topologyConfig.getRemoteSinkClusters().values().forEach(connectionEndPoints::add);
        } else {
            remoteSourceToReplicationModels.put(topologyConfig.getRemoteSourceClusters().values().stream()
                    .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                    .findFirst().get(), new HashSet<LogReplication.ReplicationModel>() {
                {
                    add(LogReplication.ReplicationModel.FULL_TABLE);
                }});
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

    private void createMultiSourceSingleSinkTopologyBuckets() {
        topologyConfig = readConfig();

        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();

        ClusterDescriptor localCluster = findLocalCluster();

        if(localCluster.getRole().equals(ClusterRole.SOURCE)) {

            remoteSinkToReplicationModels.put(topologyConfig.getRemoteSinkClusters().values().stream()
                    .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                    .findFirst().get(), new HashSet<LogReplication.ReplicationModel>() {
                {
                    add(LogReplication.ReplicationModel.FULL_TABLE);
                }});

            connectionEndPoints.add(topologyConfig.getRemoteSinkClusters().values().stream()
                    .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                    .findFirst().get());

        } else {
            remoteSourceToReplicationModels.putAll(topologyConfig.getRemoteSourceClusterToReplicationModels());
        }

        log.info("new topology:: the multi-source-single-sink: source: {} sink: {} connectionEndpoints: {}",
                remoteSourceToReplicationModels, remoteSinkToReplicationModels, connectionEndPoints);

        topologyConfig = new TopologyDescriptor(++configId, remoteSinkToReplicationModels, remoteSourceToReplicationModels,
                topologyConfig.getAllClustersInTopology(), connectionEndPoints, localNodeId);

        waitForTopologyInit.countDown();
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

        ClusterDescriptor oldLocalClusterDescriptor = findLocalCluster();


        // On role change, the role of oldCluster is flipped.
        //If old role is SOURCE (i.e., it is now the sink), it only has the remote SOURCEs connecting to it.
        //If old role is SINK (i.e., it is now the SOURCE), it has to connect to all the remote sinks.
        if(oldLocalClusterDescriptor.getRole().equals(ClusterRole.SOURCE)) {
            oldTopology.getRemoteSinkClusterToReplicationModels().forEach((cluster, value) ->
                    remoteSourceToReplicationModels.put(new ClusterDescriptor(cluster, ClusterRole.SOURCE), value));

        } else if(oldLocalClusterDescriptor.getRole().equals(ClusterRole.SINK)){
            oldTopology.getRemoteSourceClusterToReplicationModels().forEach((cluster, value) ->
                    remoteSinkToReplicationModels.put(new ClusterDescriptor(cluster, ClusterRole.SINK), value));

            connectionEndPoints.addAll(remoteSinkToReplicationModels.keySet());
        }

        Map<String, ClusterDescriptor> allRolesFlipped = new HashMap<>();
        oldTopology.getAllClustersInTopology().forEach((id, cluster) -> {
            if (cluster.getRole().equals(ClusterRole.SOURCE)) {
                allRolesFlipped.put(id, new ClusterDescriptor(cluster, ClusterRole.SINK));
            } else if (cluster.getRole().equals(ClusterRole.SINK)) {
                allRolesFlipped.put(id, new ClusterDescriptor(cluster, ClusterRole.SOURCE));
            }
        });

        log.debug("new topology :: role changed : source: {}, sink: {}, connectionEndPoints: {}",
                remoteSourceToReplicationModels, remoteSinkToReplicationModels, connectionEndPoints);

        return new TopologyDescriptor(++configId, remoteSinkToReplicationModels, remoteSourceToReplicationModels,
                allRolesFlipped, connectionEndPoints, localNodeId);
    }

    /**
     * Using the current topology, create a new topology config, which marks all sink cluster as source on purpose.
     * System should drop messages between any two source clusters.
     **/
    public TopologyDescriptor generateConfigWithAllSource() {
        Map<String, ClusterDescriptor> newSourceClusters = new HashMap<>();
        topologyConfig.getAllClustersInTopology().values().forEach(cluster -> {
            if (cluster.getRole().equals(ClusterRole.SOURCE)) {
                newSourceClusters.put(cluster.getClusterId(), cluster);
            } else {
                newSourceClusters.put(cluster.getClusterId(), new ClusterDescriptor(cluster, ClusterRole.SOURCE));
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
            if (cluster.getRole().equals(ClusterRole.SINK)) {
                newSinkClusters.put(cluster.getClusterId(), cluster);
            } else {
                newSinkClusters.put(cluster.getClusterId(), new ClusterDescriptor(cluster, ClusterRole.SINK));
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
            newInvalidClusters.put(cluster.getClusterId(), new ClusterDescriptor(cluster, ClusterRole.INVALID));
        });

        return new TopologyDescriptor(++configId, new HashMap<>(), new HashMap<>(), newInvalidClusters,
                new HashSet<>(), localNodeId);
    }

    public TopologyDescriptor generateDefaultValidConfig() {
        createSingleSourceSinkTopologyBuckets();
        return topologyConfig;
    }

    /**
     * Create a new topology config, which replaces the source cluster with a backup cluster.
     **/
    public TopologyDescriptor generateConfigWithBackup() {
        topologyConfig = readConfig();

        List<ClusterDescriptor> backupSourceClusters = new ArrayList<>();
        backupSourceClusters.add(new ClusterDescriptor(DefaultClusterConfig.getSourceClusterIds().get(1), ClusterRole.SOURCE,
            BACKUP_CORFU_PORT));

        NodeDescriptor backupNode = new NodeDescriptor(
                topology.getDefaultHost(),
                topology.getBackupLogReplicationPort(),
                DefaultClusterConfig.getSourceClusterIds().get(1),
                topology.getBackupNodesUuid().get(0),
                topology.getBackupNodesUuid().get(0)
                );
        backupSourceClusters.get(0).getNodesDescriptors().add(backupNode);

        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels = new HashMap<>();
        Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels = new HashMap<>();
        Set<ClusterDescriptor> connectionEndPoints = new HashSet<>();

        ClusterDescriptor localCluster = findLocalCluster();

        if(localCluster.getRole().equals(ClusterRole.SINK)) {
            remoteSourceToReplicationModels.put(backupSourceClusters.get(0),
                    new HashSet<LogReplication.ReplicationModel>(){
                {
                    add(LogReplication.ReplicationModel.FULL_TABLE);
                }});

        } else if (localCluster.getClusterId().equals(DefaultClusterConfig.getSourceClusterIds().get(1))){
            topologyConfig.getRemoteSinkClusters().values().stream()
                    .filter(cluster -> cluster.getClusterId().equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                    .forEach(cluster ->
                            remoteSinkToReplicationModels.put(cluster,
                                    new HashSet<LogReplication.ReplicationModel>() {
                                {
                                    add(LogReplication.ReplicationModel.FULL_TABLE);
                                }})
                    );
            connectionEndPoints.addAll(remoteSinkToReplicationModels.keySet());
        }

        // capture all the clusters
        Map<String, ClusterDescriptor> allClusters = new HashMap<>(topologyConfig.getAllClustersInTopology());
        allClusters.put(DefaultClusterConfig.getSourceClusterIds().get(1),backupSourceClusters.get(0));


        log.info("added the backup cluster: source: {} sink: {} connectionEndpoints: {}", remoteSourceToReplicationModels,
                remoteSinkToReplicationModels, connectionEndPoints);

        return new TopologyDescriptor(++configId, remoteSinkToReplicationModels, remoteSourceToReplicationModels,
                allClusters, connectionEndPoints, localNodeId);
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
                    clusterManager.createSingleSourceSinkTopologyBuckets();
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.topologyConfig);
                } else if (entry.getKey().equals(OP_ENFORCE_SNAPSHOT_FULL_SYNC)) {
                    try {
                        ClusterDescriptor localCluster = clusterManager.findLocalCluster();
                        if (localCluster.getRole().equals(ClusterRole.SINK)) {
                            return;
                        }
                        // Enforce snapshot sync on the 1st sink cluster
                        TopologyDescriptor currentTopology = clusterManager.queryTopologyConfig(true);
                        String sinkClusterId = currentTopology.getRemoteSinkClusters().keySet().stream()
                                .filter(clusterId -> clusterId.equals(DefaultClusterConfig.getSinkClusterIds().get(0)))
                                .findFirst().get();

                        String sourceClusterId = currentTopology.getRemoteSourceClusters().keySet().stream()
                                .filter(clusterId ->
                                        clusterId.equals(DefaultClusterConfig.getSourceClusterIds().get(0)))
                                .findFirst().get();

                        LogReplicationSession session = LogReplicationSession.newBuilder()
                            .setSinkClusterId(sinkClusterId)
                            .setSourceClusterId(sourceClusterId)
                            .setSubscriber(SessionManager.getDefaultSubscriber())
                            .build();
                        clusterManager.forceSnapshotSync(session);
                    } catch (LogReplicationDiscoveryServiceException e) {
                        log.warn("Caught a RuntimeException ", e);
                        ClusterRole role = clusterManager.getCorfuReplicationDiscoveryService().getLocalClusterRoleType();
                        if (role == ClusterRole.SOURCE) {
                            log.error("The current cluster role is SOURCE but forcedSnapshot Sync failed with an " +
                                    "exception", e);
                            throw new RuntimeException(e);
                        }
                    }
                } else if (entry.getKey().equals(OP_BACKUP)) {
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateConfigWithBackup());
                } else if (entry.getKey().equals(TP_SINGLE_SOURCE_SINK)) {
                    clusterManager.createSingleSourceSinkTopologyBuckets();
                } else if (entry.getKey().equals(TP_MULTI_SINK)) {
                    clusterManager.createSingleSourceMultiSinkTopologyBuckets();
                } else if (entry.getKey().equals(TP_MULTI_SOURCE)) {
                    clusterManager.createMultiSourceSingleSinkTopologyBuckets();
                }
            } else {
                log.info("onNext :: operation={}, key={}, payload={}, metadata={}", entry.getOperation().name(),
                        entry.getKey(), entry.getPayload(), entry.getMetadata());
            }
        }

        @Override
        public void onError(Throwable throwable) {
            // Ignore
        }
    }

}
