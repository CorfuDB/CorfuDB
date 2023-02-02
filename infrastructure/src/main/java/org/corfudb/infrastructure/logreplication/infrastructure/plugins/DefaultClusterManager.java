package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class extends CorfuReplicationClusterManagerAdapter, provides topology config API
 * for integration tests. The initial topology config should be valid, which means it has only
 * one source cluster, and one or more sink clusters.
 */
@Slf4j
public class DefaultClusterManager implements CorfuReplicationClusterManagerAdapter {
    private static final int BACKUP_CORFU_PORT = 9007;

    private static final String SOURCE_CLUSTER_NAME = "primary_site";
    private static final String SINK_CLUSTER_NAME = "sink_site";
    private static final String BACKUP_CLUSTER_NAME = "backup_site";

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
    private final Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> remoteSourceToReplicationModels;
    private final Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels;
    private final Set<LogReplicationClusterInfo.ClusterConfigurationMsg> connectionEndPoints;

    // Boolean to indicate if the topology triggered from test has been created for LR to consume.
    private boolean topologyBucketsInitialized;

    @Getter
    public CorfuReplicationDiscoveryService corfuReplicationDiscoveryService;

    @Getter
    public TopologyConfigurationMsg topologyConfig;

    public String localEndpoint;

    @Setter
    private String localNodeId;

    public DefaultClusterManager() {
        topology = new DefaultClusterConfig();
        remoteSourceToReplicationModels = new HashMap<>();
        remoteSinkToReplicationModels = new HashMap<>();
        connectionEndPoints = new HashSet<>();
        topologyBucketsInitialized = false;
    }

    @Override
    public void setLocalEndpoint(String endpoint) {
        this.localEndpoint = endpoint;
    }

    @Override
    public synchronized void updateTopologyConfig(TopologyConfigurationMsg newTopologyConfigMsg) {
        if (newTopologyConfigMsg.getTopologyConfigID() > topologyConfig.getTopologyConfigID()) {
            topologyConfig = newTopologyConfigMsg;
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

    public TopologyConfigurationMsg readConfig() {
        List<ClusterConfigurationMsg> sourceClusters = new ArrayList<>();
        List<ClusterConfigurationMsg> sinkClusters = new ArrayList<>();

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
                        SOURCE_CLUSTER_NAME, sourceNodeIds.get(i), sourceNodeIds.get(i));
                sourceCluster.getNodesDescriptors().add(nodeInfo);
            }
            sourceClusters.add(sourceCluster.convertToMessage());
        }

        // Setup sink cluster information
        for (int i = 0; i < sinkClusterIds.size(); i++) {
            ClusterDescriptor sinkCluster = new ClusterDescriptor(sinkClusterIds.get(i), ClusterRole.SINK,
                    Integer.parseInt(sinkCorfuPorts.get(i)));

            for (int j = 0; j < sinkNodeNames.size(); j++) {
                NodeDescriptor nodeInfo = new NodeDescriptor(sinkNodeHosts.get(i), sinkLogReplicationPorts.get(i),
                        SINK_CLUSTER_NAME, sinkNodeIds.get(i), sinkNodeIds.get(i));
                sinkCluster.getNodesDescriptors().add(nodeInfo);
            }
            sinkClusters.add(sinkCluster.convertToMessage());
        }

        return constructTopologyConfigurationMsg(0L, sourceClusters, sinkClusters, new ArrayList<>());
    }

    private TopologyConfigurationMsg constructTopologyConfigurationMsg(long configId,
                                                                       List<ClusterConfigurationMsg> sourceClusters,
                                                                       List<ClusterConfigurationMsg> sinkClusters,
                                                                       List<ClusterConfigurationMsg> invalidClusters) {
        List<ClusterConfigurationMsg> clusterConfigurationMsgs = Stream.of(sourceClusters, sinkClusters, invalidClusters)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        return TopologyConfigurationMsg.newBuilder().addAllClusters(clusterConfigurationMsgs)
                .setTopologyConfigID(configId).build();

    }


    @Override
    public TopologyConfigurationMsg queryTopologyConfig(boolean useCached) {
        if (topologyConfig == null || !useCached) {
            topologyConfig = readConfig();
        }

        log.debug("new cluster config msg " + topologyConfig);
        return topologyConfig;
    }

    public Map<LogReplicationClusterInfo.ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> getRemoteSourceToReplicationModels() {
        log.debug("getRemoteSourceToReplicationModels {}", this.remoteSourceToReplicationModels);
        return getClusterBucket(this.remoteSourceToReplicationModels);
    }


    public Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> getRemoteSinkToReplicationModels(){
        log.debug("getRemoteSinkForReplicationModels {}", this.remoteSinkToReplicationModels);
        return getClusterBucket(this.remoteSinkToReplicationModels);
    }

    private Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> getClusterBucket(
            Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> bucket) {
        Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> remoteClustersbucket;

        synchronized (this) {
            while (!topologyBucketsInitialized) {
                try {
                    log.warn("sleeping until the buckets are initialized by plugin");
                    wait();
                } catch (InterruptedException e) {
                    log.error("The thread {} has been interrupted", Thread.currentThread().getName());
                }
            }
            remoteClustersbucket = new HashMap<>(bucket);
        }
        return remoteClustersbucket;
    }

    public Set<LogReplicationClusterInfo.ClusterConfigurationMsg> fetchConnectionEndpoints() {
        Set<LogReplicationClusterInfo.ClusterConfigurationMsg> connectionEnpoints;
        synchronized (this) {
            while (!topologyBucketsInitialized) {
                try {
                    log.warn("sleeping untill the buckets are initialized by plugin");
                    wait();
                } catch (InterruptedException e) {
                    log.error("The thread {} has been interrupted", Thread.currentThread().getName());
                }
            }
            connectionEnpoints = new HashSet<>(this.connectionEndPoints);
        }
        log.debug("fetchConnectionEndpoints {}", connectionEnpoints);
        return connectionEnpoints;
    }


    private void createSingleSourceSinkTopologyBuckets() {
        if(topologyConfig == null) {
            topologyConfig = readConfig();
        }
        ClusterConfigurationMsg localCluster = findLocalCluster();
        synchronized (this) {
            if(localCluster.getRole().equals(ClusterRole.SOURCE)) {
                this.remoteSinkToReplicationModels.putIfAbsent(topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                        .findFirst().get(), new HashSet<LogReplication.ReplicationModel>(){{
                    add(LogReplication.ReplicationModel.FULL_TABLE);
                }});
                this.connectionEndPoints.add(topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                        .findFirst().get());
            } else {
                this.remoteSourceToReplicationModels.putIfAbsent(topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSourceClusterIds().get(0)))
                        .findFirst().get(), new HashSet<LogReplication.ReplicationModel>(){{
                    add(LogReplication.ReplicationModel.FULL_TABLE);
                }});
            }
            log.info("added the clusters: source: {} sink: {} connectionEndpoints: {}", this.remoteSourceToReplicationModels,
                    this.remoteSinkToReplicationModels, this.connectionEndPoints);
            topologyBucketsInitialized = true;
            notify();
        }
    }

    private void createSingleSourceMultiSinkTopologyBuckets() {
        if(topologyConfig == null) {
            topologyConfig = readConfig();
        }

        ClusterConfigurationMsg localCluster = findLocalCluster();

        synchronized (this) {
            if(localCluster.getRole().equals(ClusterRole.SOURCE)) {
                topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getRole().equals(ClusterRole.SINK))
                        .forEach(clusterConfigurationMsg -> {
                            remoteSinkToReplicationModels.putIfAbsent(clusterConfigurationMsg, new HashSet<>());
                            remoteSinkToReplicationModels.get(clusterConfigurationMsg)
                                    .add(LogReplication.ReplicationModel.FULL_TABLE);
                        });
                topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getRole().equals(ClusterRole.SINK))
                        .forEach(connectionEndPoints::add);
            } else {
                this.remoteSourceToReplicationModels.putIfAbsent(topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSourceClusterIds().get(0)))
                        .findFirst().get(), new HashSet<LogReplication.ReplicationModel>() {{
                    add(LogReplication.ReplicationModel.FULL_TABLE);
                }});
            }
            log.info("added the multi sink clusters: source: {} sink: {} connectionEndpoints: {}", this.remoteSourceToReplicationModels,
                    this.remoteSinkToReplicationModels, this.connectionEndPoints);
            topologyBucketsInitialized = true;
            notify();
        }
    }

    private ClusterConfigurationMsg findLocalCluster() {
        AtomicReference<ClusterConfigurationMsg> localCluster = new AtomicReference<>();
        topologyConfig.getClustersList().stream().forEach(clusterMsg -> {
            clusterMsg.getNodeInfoList().stream().forEach(nodeMsg -> {
                if (nodeMsg.getNodeId().equals(localNodeId)) {
                    localCluster.set(clusterMsg);
                }
            });
        });


        //true when a backup cluster becomes the source
        if(localCluster.get() == null) {
            ClusterDescriptor currentSource = new ClusterDescriptor(topologyConfig.getClustersList().stream()
                    .filter(clusterMsg -> clusterMsg.getId().equals(topology.getSourceClusterIds().get(1))).findFirst().get());

            ClusterDescriptor newLocalCluster = new ClusterDescriptor(
                    currentSource.getClusterId(), ClusterRole.SOURCE,
                    BACKUP_CORFU_PORT);
            localCluster.set(newLocalCluster.convertToMessage());
        }

        return localCluster.get();
    }

    private void createMultiSourceSingleSinkTopologyBuckets() {
        if(topologyConfig == null) {
            topologyConfig = readConfig();
        }

        ClusterConfigurationMsg localCluster = findLocalCluster();


        synchronized (this) {
            if(localCluster.getRole().equals(ClusterRole.SOURCE)) {

                this.remoteSinkToReplicationModels.putIfAbsent(topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                        .findFirst().get(), new HashSet<LogReplication.ReplicationModel>() {{
                    add(LogReplication.ReplicationModel.FULL_TABLE);
                }});

                this.connectionEndPoints.add(topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                        .findFirst().get());

            } else {
                topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getRole().equals(ClusterRole.SOURCE))
                        .forEach(clusterConfigurationMsg -> {
                            remoteSourceToReplicationModels.putIfAbsent(clusterConfigurationMsg, new HashSet<>());
                            remoteSourceToReplicationModels.get(clusterConfigurationMsg)
                                    .add(LogReplication.ReplicationModel.FULL_TABLE);
                        });
            }

            log.info("added the multi source single sink clusters: source: {} sink: {} connectionEndpoints: {}",
                    this.remoteSourceToReplicationModels, this.remoteSinkToReplicationModels, this.connectionEndPoints);
            topologyBucketsInitialized = true;
            notify();
        }
    }


    /**
     * Create a new topology config, which changes one of the sink as the source,
     * and source as sink. Data should flow in the reverse direction.
     **/
    public TopologyDescriptor generateConfigWithRoleSwitch() {
        TopologyDescriptor oldConfig = new TopologyDescriptor(topologyConfig, localNodeId, this.remoteSinkToReplicationModels,
                this.remoteSourceToReplicationModels);

        List<ClusterConfigurationMsg> newSourceClusters = new ArrayList<>();
        List<ClusterConfigurationMsg> newSinkClusters = new ArrayList<>();
        List<ClusterDescriptor> allClusters = new ArrayList<>();
        oldConfig.getAllClusterMsgsInTopology().values().forEach(clusterConfigurationMsg -> {
            if (clusterConfigurationMsg.getRole().equals(ClusterRole.SINK)) {
                newSourceClusters.add(ClusterConfigurationMsg.newBuilder(clusterConfigurationMsg)
                        .setRole(ClusterRole.SOURCE).build());
                allClusters.add(new ClusterDescriptor(new ClusterDescriptor(clusterConfigurationMsg), ClusterRole.SOURCE));
            } else if (clusterConfigurationMsg.getRole().equals(ClusterRole.SOURCE)) {
                newSinkClusters.add(ClusterConfigurationMsg.newBuilder(clusterConfigurationMsg)
                        .setRole(ClusterRole.SINK).build());
                allClusters.add(new ClusterDescriptor(new ClusterDescriptor(clusterConfigurationMsg), ClusterRole.SINK));
            }
        });

        ClusterDescriptor oldLocalClusterDescriptor = oldConfig.getLocalClusterDescriptor();
        List<ClusterDescriptor> remoteSourceClusterList = new ArrayList<>();
        List<ClusterDescriptor> remoteSinkClusterList = new ArrayList<>();

        synchronized (this) {
            // if the current one is Source => give only remote sink; but if the current one is sink, remote sink should be empty
            Set<ClusterConfigurationMsg> newSourceClusterMsg = new HashSet<>();
            Set<ClusterConfigurationMsg> newSinkClusterMsg = new HashSet<>();

            this.remoteSinkToReplicationModels.keySet().stream().forEach(clusterMsg -> {
                ClusterConfigurationMsg newClusterMsg = ClusterConfigurationMsg
                        .newBuilder(clusterMsg).setRole(ClusterRole.SOURCE).build();
                newSourceClusterMsg.add(newClusterMsg);
            });
            this.remoteSourceToReplicationModels.keySet().stream().forEach(clusterMsg -> {
                ClusterConfigurationMsg newClusterMsg = ClusterConfigurationMsg
                        .newBuilder(clusterMsg).setRole(ClusterRole.SINK).build();
                newSinkClusterMsg.add(newClusterMsg);
            });
            this.remoteSinkToReplicationModels.clear();
            this.remoteSourceToReplicationModels.clear();
            this.connectionEndPoints.clear();

            // On role change, the role of oldCluster is flipped.
            //If old role is SOURCE (i.e., it is now the sink), it only has the remote SOURCEs connecting to it.
            //If old role is SINK (i.e., it is now the SOURCE), it has to connect to all the remote sinks.
            if(oldLocalClusterDescriptor.getRole().equals(ClusterRole.SOURCE)) {
                newSourceClusterMsg.stream().forEach(clusterConfigurationMsg -> {
                    remoteSourceToReplicationModels.putIfAbsent(clusterConfigurationMsg, new HashSet<>());
                    remoteSourceToReplicationModels.get(clusterConfigurationMsg).add(LogReplication.ReplicationModel.FULL_TABLE);
                    remoteSourceClusterList.add(new ClusterDescriptor(clusterConfigurationMsg));
                });

            } else if(oldLocalClusterDescriptor.getRole().equals(ClusterRole.SINK)){
                newSinkClusterMsg.stream().forEach(clusterConfigurationMsg -> {
                    remoteSinkToReplicationModels.putIfAbsent(clusterConfigurationMsg, new HashSet<>());
                    remoteSinkToReplicationModels.get(clusterConfigurationMsg).add(LogReplication.ReplicationModel.FULL_TABLE);
                    remoteSinkClusterList.add(new ClusterDescriptor(clusterConfigurationMsg));
                });
                this.connectionEndPoints.addAll(newSinkClusterMsg);
            }
        }

        log.debug("the topology has source: {}, sink: {}, connectionEndPoints: {}", this.remoteSourceToReplicationModels,
                this.remoteSinkToReplicationModels, this.connectionEndPoints);
        return new TopologyDescriptor(++configId, remoteSourceClusterList, remoteSinkClusterList, allClusters, localNodeId);
    }

    /**
     * Create a new topology config, which marks all sink cluster as source on purpose.
     * System should drop messages between any two source clusters.
     **/
    public TopologyDescriptor generateConfigWithAllSource() {
        topologyConfig = readConfig();
        List<ClusterDescriptor> newSourceClusters = new ArrayList<>();
        topologyConfig.getClustersList().stream().forEach(clusterMsg -> {
            if (clusterMsg.getRole().equals(ClusterRole.SOURCE)) {
                newSourceClusters.add(new ClusterDescriptor(clusterMsg));
            } else {
                newSourceClusters.add(new ClusterDescriptor(new ClusterDescriptor(clusterMsg), ClusterRole.SOURCE));
            }
        });
        resetBuckets();

        return new TopologyDescriptor(++configId, new ArrayList<>(), new ArrayList<>(), newSourceClusters, localNodeId);
    }

    private void resetBuckets() {
        synchronized (this) {
            this.remoteSourceToReplicationModels.clear();
            this.remoteSinkToReplicationModels.clear();
            this.connectionEndPoints.clear();
        }
    }

    /**
     * Create a new topology config, which marks all cluster as sink on purpose.
     * System should not send messages in this case.
     **/
    public TopologyDescriptor generateConfigWithAllSink() {
        topologyConfig = readConfig();
        ClusterDescriptor currentSource = new ClusterDescriptor(topologyConfig.getClustersList().stream()
                .filter(clusterMsg -> clusterMsg.getId().equals(topology.getSourceClusterIds().get(0)))
                .findFirst().get());


        List<ClusterDescriptor> allSinkClusters = new ArrayList<>();
        topologyConfig.getClustersList().stream()
                .forEach(clusterMsg -> allSinkClusters.add(new ClusterDescriptor(clusterMsg)));
        ClusterDescriptor newSink = new ClusterDescriptor(currentSource, ClusterRole.SINK);
        allSinkClusters.add(newSink);

        resetBuckets();

        return new TopologyDescriptor(++configId, new ArrayList<>(), new ArrayList<>(), allSinkClusters, localNodeId);
    }

    /**
     * Create a new topology config, which marks all sink cluster as invalid on purpose.
     * LR should not replicate to these clusters.
     **/
    public TopologyDescriptor generateConfigWithInvalid() {

        List<ClusterDescriptor> newInvalidClusters = new ArrayList<>();
        topologyConfig.getClustersList().stream()
                .forEach(clusterMsg -> newInvalidClusters.add(new ClusterDescriptor(clusterMsg)));

        //Since all the sinks are marked as invalid, the remote source/sink/connectionEndPoints will all be empty as
        //(i) if local cluster is source, there are no sinks and hence no connectionEndPoints
        //(ii) if the local cluster is INVALID (previously sink), no LR against it.
        resetBuckets();

        return new TopologyDescriptor(++configId, new ArrayList<>(), new ArrayList<>(), newInvalidClusters, localNodeId);
    }

    /**
     * Bring topology config back to default valid config.
     **/
    public TopologyDescriptor generateDefaultValidConfig() {
        topologyConfig = readConfig();
        resetBuckets();
        createSingleSourceSinkTopologyBuckets();

        TopologyDescriptor defaultTopology = new TopologyDescriptor(readConfig(),localNodeId, this.remoteSinkToReplicationModels,
                this.remoteSourceToReplicationModels);
        List<ClusterDescriptor> sourceClusters = new ArrayList<>(defaultTopology.getRemoteSourceClusters().values());
        List<ClusterDescriptor> sinkClusters = new ArrayList<>(defaultTopology.getRemoteSinkClusters().values());
        List<ClusterDescriptor> allClusters = new ArrayList<>();
        topologyConfig.getClustersList().stream()
                .forEach(clusterConfigurationMsg -> allClusters.add(new ClusterDescriptor(clusterConfigurationMsg)));


        ClusterConfigurationMsg localCluster = findLocalCluster();

        synchronized (this) {
            if (localCluster.getRole().equals(ClusterRole.SOURCE)) {
                sinkClusters.stream().forEach(clusterDescriptor -> {
                    this.remoteSinkToReplicationModels.putIfAbsent(clusterDescriptor.convertToMessage(), new HashSet<>());
                    this.remoteSinkToReplicationModels.get(clusterDescriptor.convertToMessage())
                            .add(LogReplication.ReplicationModel.FULL_TABLE);

                    this.connectionEndPoints.add(clusterDescriptor.convertToMessage());
                });
                sourceClusters.clear();

            } else {
                sourceClusters.stream().forEach(clusterDescriptor -> {
                    this.remoteSourceToReplicationModels.putIfAbsent(clusterDescriptor.convertToMessage(), new HashSet<>());
                    this.remoteSourceToReplicationModels.get(clusterDescriptor.convertToMessage())
                            .add(LogReplication.ReplicationModel.FULL_TABLE);
                });
                sinkClusters.clear();
            }
        }

        return new TopologyDescriptor(++configId, sourceClusters, sinkClusters, allClusters, localNodeId);
    }

    /**
     * Update the valid default topology config with sink cluster.
     * Add one sink cluster and Remove existing sink cluster.
     **/
    @VisibleForTesting
    public TopologyDescriptor updateDefaultValidConfig() {
        TopologyDescriptor defaultTopology = new TopologyDescriptor(constructTopologyConfigMsg(), localEndpoint);
        List<ClusterDescriptor> sourceClusters = new ArrayList<>(defaultTopology.getSourceClusters().values());
        List<ClusterDescriptor> sinkClusters = new ArrayList<>(defaultTopology.getSinkClusters().values());
        int sinkClusterSize = sinkClusters.size();
        // Given the sink cluster size is 3 already. It is safe to remove the middle cluster.
        if(sinkClusterSize > 2) {
            sinkClusters.remove(sinkClusters.get(sinkClusterSize - 2));
        }
        sinkClusters.add(new ClusterDescriptor("new-test-sink-cluster-id", ClusterRole.SINK,
                Integer.parseInt(topology.getSinkCorfuPorts().get(0))));

        return new TopologyDescriptor(++configId, sourceClusters, sinkClusters, localEndpoint);
    }

    /**
     * Create a new topology config, which replaces the source cluster with a backup cluster.
     **/
    public TopologyDescriptor generateConfigWithBackup() {
        topologyConfig = readConfig();

        List<ClusterDescriptor> backupSourceClusters = new ArrayList<>();
        backupSourceClusters.add(new ClusterDescriptor(topology.getSourceClusterIds().get(1), ClusterRole.SOURCE,
            BACKUP_CORFU_PORT));

        NodeDescriptor backupNode = new NodeDescriptor(
                topology.getDefaultHost(),
                topology.getBackupLogReplicationPort(),
                BACKUP_CLUSTER_NAME,
                topology.getBackupNodesUuid().get(0),
                topology.getBackupNodesUuid().get(0)
                );
        backupSourceClusters.get(0).getNodesDescriptors().add(backupNode);
        List<ClusterDescriptor> sinkClusters = new ArrayList<>();
        List<ClusterDescriptor> sourceClusters = new ArrayList<>();

        topologyConfig.getClustersList().stream().filter(clusterMsg -> clusterMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                .forEach(clusterMsg -> sinkClusters.add(new ClusterDescriptor(clusterMsg)));

        ClusterConfigurationMsg localCluster = findLocalCluster();

        synchronized (this) {
            if(localCluster.getRole().equals(ClusterRole.SINK)) {

                this.remoteSourceToReplicationModels.clear();
                this.remoteSourceToReplicationModels.putIfAbsent(backupSourceClusters.get(0).convertToMessage(), new HashSet<>());
                this.remoteSourceToReplicationModels.get(backupSourceClusters.get(0).convertToMessage())
                        .add(LogReplication.ReplicationModel.FULL_TABLE);
                sourceClusters.add(backupSourceClusters.get(0));

                this.remoteSinkToReplicationModels.clear();
                this.connectionEndPoints.clear();

            } else if (localCluster.getId().equals(topology.getSourceClusterIds().get(1))){
                topologyConfig.getClustersList().stream()
                        .filter(clusterMsg -> clusterMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                        .forEach(clusterMsg -> {
                            ClusterConfigurationMsg configurationMsg = clusterMsg;
                            this.remoteSinkToReplicationModels.putIfAbsent(configurationMsg, new HashSet<>());
                            this.remoteSinkToReplicationModels.get(configurationMsg).add(LogReplication.ReplicationModel.FULL_TABLE);
                            sinkClusters.add(new ClusterDescriptor(clusterMsg));

                            this.remoteSourceToReplicationModels.clear();
                            this.connectionEndPoints.clear();

                            this.connectionEndPoints.add(configurationMsg);
                });
            }
        }

        // capture all the clusters
        List<ClusterDescriptor> allClusters = new ArrayList<>();
        allClusters.add(backupSourceClusters.get(0));
        topologyConfig.getClustersList().stream()
                .filter(clusterMsg -> clusterMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                .forEach(clusterConfigurationMsg -> allClusters.add(new ClusterDescriptor(clusterConfigurationMsg)));

        log.info("added the backup cluster: source: {} sink: {} connectionEndpoints: {}", this.remoteSourceToReplicationModels,
                this.remoteSinkToReplicationModels, this.connectionEndPoints);

        return new TopologyDescriptor(++configId, sourceClusters, sinkClusters, allClusters, localNodeId);
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
                    clusterManager.updateTopologyConfig(newConfig.convertToMessage());
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
                            .applyNewTopologyConfig(clusterManager.generateDefaultValidConfig());
                } else if (entry.getKey().equals(OP_ENFORCE_SNAPSHOT_FULL_SYNC)) {
                    try {
                        ClusterConfigurationMsg localCluster = clusterManager.findLocalCluster();
                        if (localCluster.getRole().equals(ClusterRole.SINK)) {
                            return;
                        }
                        // Enforce snapshot sync on the 1st sink cluster
                        List<ClusterConfigurationMsg> clusters =
                            clusterManager.queryTopologyConfig(true).getClustersList();
                        Optional<ClusterConfigurationMsg> sinkCluster =
                            clusters.stream().filter(cluster -> cluster.getRole() == ClusterRole.SINK
                            && cluster.getId().equals(clusterManager.topology.getSinkClusterIds().get(0))).findFirst();

                        Optional<ClusterConfigurationMsg> sourceCluster =
                            clusters.stream().filter(cluster -> cluster.getRole() == ClusterRole.SOURCE
                                && cluster.getId().equals(clusterManager.topology.getSourceClusterIds().get(0))).findFirst();

                        LogReplicationSession session = LogReplicationSession.newBuilder()
                            .setSinkClusterId(sinkCluster.get().getId())
                            .setSourceClusterId(sourceCluster.get().getId())
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
