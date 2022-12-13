package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
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
public class DefaultClusterManager extends CorfuReplicationClusterManagerBaseAdapter {
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
    public static final ClusterUuidMsg OP_SINGLE_SOURCE_SINK = ClusterUuidMsg.newBuilder().setLsb(7L).setMsb(7L).build();
    public static final ClusterUuidMsg OP_MULTI_SINK = ClusterUuidMsg.newBuilder().setLsb(8L).setMsb(8L).build();
    public static final ClusterUuidMsg OP_MULTI_SOURCE = ClusterUuidMsg.newBuilder().setLsb(9L).setMsb(9L).build();
    public static final ClusterUuidMsg OP_BIDIR_ORPH_SINK = ClusterUuidMsg.newBuilder().setLsb(10L).setMsb(10L).build();
    public static final ClusterUuidMsg OP_SINK_CONNECT_INIT = ClusterUuidMsg.newBuilder().setLsb(11L).setMsb(11L).build();

    @Getter
    private long configId;

    @Getter
    private boolean shutdown;

    @Getter
    public ClusterManagerCallback clusterManagerCallback;

    private CorfuRuntime corfuRuntime;

    private CorfuStore corfuStore;

    private ConfigStreamListener configStreamListener;

    private String corfuEndpoint = "localhost:9000";

    private DefaultClusterConfig topology;
    private final Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> remoteSourcesToReplicationModels;
    private final Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> remoteSinkToReplicationModels;
    private final Set<LogReplicationClusterInfo.ClusterConfigurationMsg> connectionEndPoints;
    private boolean topologyBucketsIinitialized;
    private boolean topologyBucketsCustomized;

    public DefaultClusterManager() {
        topology = new DefaultClusterConfig();
        remoteSourcesToReplicationModels = new HashMap<>();
        remoteSinkToReplicationModels = new HashMap<>();
        connectionEndPoints = new HashSet<>();
        topologyBucketsIinitialized = false;
        topologyBucketsCustomized = false;
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

        return constructTopologyConfigurationMsg(0L,sourceClusters, sinkClusters, new ArrayList<>());
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
        log.debug("getRemoteSourceToReplicationModels {}", this.remoteSourcesToReplicationModels);
        return getClusterBucket(this.remoteSourcesToReplicationModels);
    }


    public Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> getRemoteSinkForReplicationModels(){
        log.debug("getRemoteSinkForReplicationModels {}", this.remoteSinkToReplicationModels);
        return getClusterBucket(this.remoteSinkToReplicationModels);
    }

    private Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> getClusterBucket(
            Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> bucket) {
        Map<ClusterConfigurationMsg, Set<LogReplication.ReplicationModel>> remoteClustersbucket;

        ClusterConfigurationMsg localCluster = findLocalCluster();
        synchronized (this) {
            while (!topologyBucketsIinitialized) {
                try {
                    log.warn("sleeping untill the buckets are initialized by plugin");
                    wait();
                } catch (InterruptedException e) {
                    log.error("The thread {} has been interrupted", Thread.currentThread().getName());
                }
            }
            if (!topologyBucketsCustomized) {
                customizeBuckets(localCluster);
            }
            remoteClustersbucket = new HashMap<>(bucket);
        }
        return remoteClustersbucket;
    }

    private void customizeBuckets(ClusterConfigurationMsg localCluster) {
        if (localCluster.getRole().equals(ClusterRole.SOURCE)) {
            remoteSourcesToReplicationModels.clear();
        } else {
            remoteSinkToReplicationModels.clear();
            connectionEndPoints.clear();
        }

        topologyBucketsCustomized = true;
    }

    public Set<LogReplicationClusterInfo.ClusterConfigurationMsg> fetchConnectionEndpoints() {
        Set<LogReplicationClusterInfo.ClusterConfigurationMsg> connectionEnpoints;
        ClusterConfigurationMsg localCluster = findLocalCluster();
        synchronized (this) {
            while (!topologyBucketsIinitialized) {
                try {
                    log.warn("sleeping untill the buckets are initialized by plugin");
                    wait();
                } catch (InterruptedException e) {
                    log.error("The thread {} has been interrupted", Thread.currentThread().getName());
                }
            }
            if (!topologyBucketsCustomized) {
                customizeBuckets(localCluster);
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
        synchronized (this) {
            this.remoteSourcesToReplicationModels.putIfAbsent(topologyConfig.getClustersList().stream()
                    .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSourceClusterIds().get(0)))
                    .findFirst().get(), new HashSet<LogReplication.ReplicationModel>(){{
                        add(LogReplication.ReplicationModel.FULL_TABLE);
                    }});
            this.remoteSinkToReplicationModels.putIfAbsent(topologyConfig.getClustersList().stream()
                    .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                    .findFirst().get(), new HashSet<LogReplication.ReplicationModel>(){{
                add(LogReplication.ReplicationModel.FULL_TABLE);
            }});
            this.connectionEndPoints.add(topologyConfig.getClustersList().stream()
                    .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                    .findFirst().get());
            log.info("added the clusters: source: {} sink: {} connectionEndpoints: {}", this.remoteSourcesToReplicationModels,
                    this.remoteSinkToReplicationModels, this.connectionEndPoints);
            topologyBucketsIinitialized = true;
            notify();
        }
    }

    private void createSingleSourceMultiSinkTopologyBuckets() {
        if(topologyConfig == null) {
            topologyConfig = readConfig();
        }

        synchronized (this) {
            this.remoteSourcesToReplicationModels.putIfAbsent(topologyConfig.getClustersList().stream()
                    .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSourceClusterIds().get(0)))
                    .findFirst().get(), new HashSet<LogReplication.ReplicationModel>(){{
                        add(LogReplication.ReplicationModel.FULL_TABLE);
            }});

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
            log.info("added the multi sink clusters: source: {} sink: {} connectionEndpoints: {}", this.remoteSourcesToReplicationModels,
                    this.remoteSinkToReplicationModels, this.connectionEndPoints);
            topologyBucketsIinitialized = true;
            notify();
        }
    }

    private ClusterConfigurationMsg findLocalCluster() {
        String localNodeId = corfuReplicationDiscoveryService.getLocalNodeId();
        log.debug("localNodeId {}", localNodeId );
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
                    .filter(clusterMsg -> clusterMsg.getId().equals(topology.getSourceClusterIds().get(0))).findFirst().get());

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

        synchronized (this) {
            topologyConfig.getClustersList().stream()
                    .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getRole().equals(ClusterRole.SOURCE))
                    .forEach(clusterConfigurationMsg -> {
                        remoteSourcesToReplicationModels.putIfAbsent(clusterConfigurationMsg, new HashSet<>());
                        remoteSourcesToReplicationModels.get(clusterConfigurationMsg)
                                .add(LogReplication.ReplicationModel.FULL_TABLE);
                    });


            this.remoteSinkToReplicationModels.putIfAbsent(topologyConfig.getClustersList().stream()
                    .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                    .findFirst().get(), new HashSet<LogReplication.ReplicationModel>(){{
                        add(LogReplication.ReplicationModel.FULL_TABLE);
            }});

            this.connectionEndPoints.add(topologyConfig.getClustersList().stream()
                    .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                    .findFirst().get());
            log.info("added the multi source single sink clusters: source: {} sink: {} connectionEndpoints: {}",
                    this.remoteSourcesToReplicationModels, this.remoteSinkToReplicationModels, this.connectionEndPoints);
            topologyBucketsIinitialized = true;
            notify();
        }
    }

    private void createBiDirectionalButOrphanedSink() {
        if(topologyConfig == null) {
            topologyConfig = readConfig();
        }

        ClusterConfigurationMsg localCluster = findLocalCluster();
        log.info("localNodeId: {}", localCluster);

        synchronized (this) {
            if(localCluster.getId().equals(topology.getSourceClusterIds().get(0))) {

                this.remoteSourcesToReplicationModels.putIfAbsent(topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSourceClusterIds().get(1)))
                        .findFirst().get(), new HashSet<LogReplication.ReplicationModel>(){{
                            //replication model chosen at random here
                            add(LogReplication.ReplicationModel.MULTI_SOURCE_MERGE);
                }});

                this.remoteSinkToReplicationModels.putIfAbsent(topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                        .findFirst().get(), new HashSet<LogReplication.ReplicationModel>(){{
                            add(LogReplication.ReplicationModel.FULL_TABLE);
                }});

                this.connectionEndPoints.add(topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                        .findFirst().get());

            } else {
                // have only sink
                this.remoteSourcesToReplicationModels.putIfAbsent(topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSourceClusterIds().get(0)))
                        .findFirst().get(), new HashSet<LogReplication.ReplicationModel>(){{
                            add(LogReplication.ReplicationModel.FULL_TABLE);
                }});
            }

            topologyBucketsIinitialized = true;
            topologyBucketsCustomized = true;

            notify();
        }
    }

    private void createSinkConnectionInit() {
        if(topologyConfig == null) {
            topologyConfig = readConfig();
        }

        ClusterConfigurationMsg localCluster = findLocalCluster();
        log.info("localNodeId: {}", localCluster);

        synchronized (this) {
            if(localCluster.getId().equals(topology.getSourceClusterIds().get(0))) {

                this.remoteSinkToReplicationModels.putIfAbsent(topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                        .findFirst().get(), new HashSet<LogReplication.ReplicationModel>(){{
                    add(LogReplication.ReplicationModel.FULL_TABLE);
                }});

            } else {
                // have only sink
                this.remoteSourcesToReplicationModels.putIfAbsent(topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSourceClusterIds().get(0)))
                        .findFirst().get(), new HashSet<LogReplication.ReplicationModel>(){{
                    add(LogReplication.ReplicationModel.FULL_TABLE);
                }});

                this.connectionEndPoints.add(topologyConfig.getClustersList().stream()
                        .filter(clusterConfigurationMsg -> clusterConfigurationMsg.getId().equals(topology.getSourceClusterIds().get(0)))
                        .findFirst().get());
            }

            topologyBucketsIinitialized = true;
            topologyBucketsCustomized = true;

            notify();
        }
    }


    /**
     * Create a new topology config, which changes one of the sink as the source,
     * and source as sink. Data should flow in the reverse direction.
     **/
    public TopologyDescriptor generateConfigWithRoleSwitch() {
        TopologyDescriptor currentConfig = new TopologyDescriptor(topologyConfig, this);

        List<ClusterConfigurationMsg> newSourceClusters = new ArrayList<>();
        List<ClusterConfigurationMsg> newSinkClusters = new ArrayList<>();
        List<ClusterConfigurationMsg> newInvalidClusters = new ArrayList<>();
        currentConfig.getAllClusterConfigMsgsInTopology().values().forEach(clusterConfigurationMsg -> {
            if (clusterConfigurationMsg.getRole().equals(ClusterRole.SINK)) {
                newSourceClusters.add(ClusterConfigurationMsg.newBuilder(clusterConfigurationMsg)
                        .setRole(ClusterRole.SOURCE).build());
            } else if (clusterConfigurationMsg.getRole().equals(ClusterRole.SOURCE)) {
                newSinkClusters.add(ClusterConfigurationMsg.newBuilder(clusterConfigurationMsg)
                        .setRole(ClusterRole.SINK).build());
            } else {
                newInvalidClusters.add(clusterConfigurationMsg);
            }
        });

        ClusterDescriptor oldLocalClusterDescriptor = getCorfuReplicationDiscoveryService().getLocalCluster();

        synchronized (this) {
            // if the current one is Source => give only remote sink; but if the current one is sink, remote sink should be empty
            Set<ClusterConfigurationMsg> newSourceClusterMsg = new HashSet<>();
            Set<ClusterConfigurationMsg> newSinkClusterMsg = new HashSet<>();

            this.remoteSinkToReplicationModels.keySet().stream().forEach(clusterMsg -> {
                ClusterConfigurationMsg newClusterMsg = ClusterConfigurationMsg
                        .newBuilder(clusterMsg).setRole(ClusterRole.SOURCE).build();
                newSourceClusterMsg.add(newClusterMsg);
            });
            this.remoteSourcesToReplicationModels.keySet().stream().forEach(clusterMsg -> {
                ClusterConfigurationMsg newClusterMsg = ClusterConfigurationMsg
                        .newBuilder(clusterMsg).setRole(ClusterRole.SINK).build();
                newSinkClusterMsg.add(newClusterMsg);
            });
            this.remoteSinkToReplicationModels.clear();
            this.remoteSourcesToReplicationModels.clear();
            this.connectionEndPoints.clear();

            // On role change, the role of oldCluster is flipped.
            //If old role is SOURCE (i.e., it is now the sink), it only has the remote SOURCEs connecting to it.
            //If old role is SINK (i.e., it is now the SOURCE), it has to connect to all the remote sinks.
            if(oldLocalClusterDescriptor.getRole().equals(ClusterRole.SOURCE)) {
                newSourceClusterMsg.stream().forEach(clusterConfigurationMsg ->
                        remoteSourcesToReplicationModels.put(clusterConfigurationMsg,
                                new HashSet<LogReplication.ReplicationModel>(){{
                                    add(LogReplication.ReplicationModel.FULL_TABLE);
                        }}));
            } else if(oldLocalClusterDescriptor.getRole().equals(ClusterRole.SINK)){
                newSinkClusterMsg.stream().forEach(clusterConfigurationMsg ->
                        remoteSinkToReplicationModels.put(clusterConfigurationMsg,
                                new HashSet<LogReplication.ReplicationModel>(){{
                                    add(LogReplication.ReplicationModel.FULL_TABLE);
                        }}));
                this.connectionEndPoints.addAll(newSinkClusterMsg);
            }

            topologyBucketsCustomized = true;
        }
        log.debug("the topology has source: {}, sink: {}, connectionEndPoints: {}", this.remoteSourcesToReplicationModels, this.remoteSinkToReplicationModels, this.connectionEndPoints);
        return new TopologyDescriptor(
                constructTopologyConfigurationMsg(++configId, newSourceClusters, newSinkClusters, new ArrayList<>()),
                this);
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

        ClusterDescriptor oldLocalClusterDescriptor = getCorfuReplicationDiscoveryService().getLocalCluster();
        resetBuckets();

        synchronized (this) {
            if(oldLocalClusterDescriptor.getRole().equals(ClusterRole.SINK)) {
                newSourceClusters.forEach(sourceCluster ->
                        this.remoteSourcesToReplicationModels.put(sourceCluster.convertToMessage(),
                                new HashSet<LogReplication.ReplicationModel>(){{
                                    add(LogReplication.ReplicationModel.FULL_TABLE); }}
                        ));
            }
        }

        return new TopologyDescriptor(++configId, newSourceClusters, new ArrayList<>());
    }

    private void resetBuckets() {
        synchronized (this) {
            this.remoteSourcesToReplicationModels.clear();
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


        List<ClusterDescriptor> newSinkClusters = new ArrayList<>();
        topologyConfig.getClustersList().stream().filter(clusterMsg -> clusterMsg.getRole().equals(ClusterRole.SINK))
                .forEach(clusterMsg -> newSinkClusters.add(new ClusterDescriptor(clusterMsg)));
        ClusterDescriptor newSink = new ClusterDescriptor(currentSource, ClusterRole.SINK);
        newSinkClusters.add(newSink);

        resetBuckets();
        synchronized (this) {
            topologyBucketsCustomized = false;
        }

        return new TopologyDescriptor(++configId, new ArrayList<>(), newSinkClusters);
    }

    /**
     * Create a new topology config, which marks all sink cluster as invalid on purpose.
     * LR should not replicate to these clusters.
     **/
    public TopologyDescriptor generateConfigWithInvalid() {

        List<ClusterDescriptor> newSourceClusters = new ArrayList<>();
        List<ClusterDescriptor> newInvalidClusters = new ArrayList<>();
        topologyConfig.getClustersList().stream()
                .forEach(clusterMsg -> {
                    if (clusterMsg.getRole().equals(ClusterRole.SOURCE)) {
                        newSourceClusters.add(new ClusterDescriptor(clusterMsg));
                    } else if (clusterMsg.getRole().equals(ClusterRole.SINK)) {
                        newInvalidClusters.add(new ClusterDescriptor(new ClusterDescriptor(clusterMsg), ClusterRole.INVALID));
                    }
                });

        //Since all the sinks are marked as invalid, the remote source/sink/connectionEndPoints will all be empty as
        //(i) if local cluster is source, there are no sinks and hence no connectionEndPoints
        //(ii) if the local cluster is INVALID (previously sink), no LR against it.
        resetBuckets();

        return new TopologyDescriptor(++configId, newSourceClusters, new ArrayList<>(), newInvalidClusters);
    }

    /**
     * Bring topology config back to default valid config.
     **/
    public TopologyDescriptor generateDefaultValidConfig() {
        topologyConfig = readConfig();
        resetBuckets();
        createSingleSourceSinkTopologyBuckets();

        TopologyDescriptor defaultTopology = new TopologyDescriptor(readConfig(), this);
        List<ClusterDescriptor> sourceClusters = new ArrayList<>(defaultTopology.getRemoteSourceClusters().values());
        List<ClusterDescriptor> sinkClusters = new ArrayList<>(defaultTopology.getRemoteSinkClusters().values());

        // topologyBucketsCustomized=false, so the buckets are defined as per the local cluster's role on getRemote calls
        synchronized (this) {
            topologyBucketsCustomized = false;
        }
        return new TopologyDescriptor(++configId, sourceClusters, sinkClusters);
    }

    /**
     * Create a new topology config, which replaces the source cluster with a backup cluster.
     **/
    public TopologyDescriptor generateConfigWithBackup() {
        topologyConfig = readConfig();

        ClusterDescriptor currentSource = new ClusterDescriptor(topologyConfig.getClustersList().stream()
                .filter(clusterMsg -> clusterMsg.getId().equals(topology.getSourceClusterIds().get(0))).findFirst().get());


        List<ClusterDescriptor> newSourceClusters = new ArrayList<>();
        newSourceClusters.add(new ClusterDescriptor(
            currentSource.getClusterId(), ClusterRole.SOURCE,
            BACKUP_CORFU_PORT));

        NodeDescriptor backupNode = new NodeDescriptor(
                topology.getDefaultHost(),
                topology.getBackupLogReplicationPort(),
                BACKUP_CLUSTER_NAME,
                topology.getBackupNodesUuid().get(0),
                topology.getBackupNodesUuid().get(0)
                );
        newSourceClusters.get(0).getNodesDescriptors().add(backupNode);
        List<ClusterDescriptor> sinkClusters = new ArrayList<>();
        topologyConfig.getClustersList().stream().filter(clusterMsg -> clusterMsg.getId().equals(topology.getSinkClusterIds().get(0)))
                .forEach(clusterMsg -> sinkClusters.add(new ClusterDescriptor(clusterMsg)));

        synchronized (this) {
            sinkClusters.stream().forEach(cluster ->
                this.remoteSinkToReplicationModels.putIfAbsent(cluster.convertToMessage(),
                        new HashSet<LogReplication.ReplicationModel>(){{
                            add(LogReplication.ReplicationModel.FULL_TABLE);
                        }}
                )
            );

            this.remoteSourcesToReplicationModels.put(currentSource.convertToMessage(),
                    new HashSet<LogReplication.ReplicationModel>(){{
                        add(LogReplication.ReplicationModel.FULL_TABLE);
            }});

            topologyBucketsCustomized = false;
        }
        return new TopologyDescriptor(++configId, newSourceClusters, sinkClusters);
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
                        // Enforce snapshot sync on the 1st sink cluster
                        List<ClusterConfigurationMsg> clusters =
                            clusterManager.queryTopologyConfig(true).getClustersList();
                        Optional<ClusterConfigurationMsg> sinkCluster =
                            clusters.stream().filter(cluster -> cluster.getRole() == ClusterRole.SINK
                            && cluster.getId().equals(clusterManager.topology.getSinkClusterIds().get(0))).findFirst();
                        clusterManager.forceSnapshotSync(
                            sinkCluster.get().getId());
                    } catch (LogReplicationDiscoveryServiceException e) {
                        log.warn("Caught a RuntimeException ", e);
                        ClusterRole role = clusterManager.getCorfuReplicationDiscoveryService().getLocalClusterRoleType();
                        if (role != ClusterRole.SINK) {
                            log.error("The current cluster role is {} and should not throw a RuntimeException for forceSnapshotSync call.", role);
                            Thread.interrupted();
                        }
                    }
                } else if (entry.getKey().equals(OP_BACKUP)) {
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateConfigWithBackup());
                } else if (entry.getKey().equals(OP_SINGLE_SOURCE_SINK)) {
                    clusterManager.createSingleSourceSinkTopologyBuckets();
                } else if (entry.getKey().equals(OP_MULTI_SINK)) {
                    clusterManager.createSingleSourceMultiSinkTopologyBuckets();
                } else if (entry.getKey().equals(OP_MULTI_SOURCE)) {
                    clusterManager.createMultiSourceSingleSinkTopologyBuckets();
                } else if (entry.getKey().equals(OP_BIDIR_ORPH_SINK)) {
                    clusterManager.createBiDirectionalButOrphanedSink();
                } else if(entry.getKey().equals(OP_SINK_CONNECT_INIT)) {
                    clusterManager.createSinkConnectionInit();
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
