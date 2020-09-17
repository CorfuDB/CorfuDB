package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.Messages;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.utils.CommonTypes;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class extends CorfuReplicationClusterManagerAdapter, provides topology config API
 * for integration tests. The initial topology config should be valid, which means it has only
 * one active cluster, and one or more standby clusters.
 */
@Slf4j
public class DefaultClusterManager extends CorfuReplicationClusterManagerBaseAdapter {
    public static final String CONFIG_FILE_PATH = "src/test/resources/corfu_replication_config.properties";
    private static final String DEFAULT_ACTIVE_CLUSTER_NAME = "primary_site";
    private static final String DEFAULT_STANDBY_CLUSTER_NAME = "standby_site";

    private static final int NUM_NODES_PER_CLUSTER = 3;

    private static final String ACTIVE_CLUSTER_NAME = "primary_site";
    private static final String STANDBY_CLUSTER_NAME = "standby_site";
    private static final String ACTIVE_CLUSTER_CORFU_PORT = "primary_site_corfu_portnumber";
    private static final String STANDBY_CLUSTER_CORFU_PORT = "standby_site_corfu_portnumber";
    private static final String LOG_REPLICATION_SERVICE_ACTIVE_PORT_NUM = "primary_site_portnumber";
    private static final String LOG_REPLICATION_SERVICE_STANDBY_PORT_NUM = "standby_site_portnumber";

    private static final String ACTIVE_CLUSTER_NODE = "primary_site_node";
    private static final String STANDBY_CLUSTER_NODE = "standby_site_node";


    public static final String CONFIG_NAMESPACE = "ns_lr_config_it";
    public static final String CONFIG_TABLE_NAME = "lr_config_it";
    public static final CommonTypes.Uuid OP_RESUME = CommonTypes.Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
    public static final CommonTypes.Uuid OP_SWITCH = CommonTypes.Uuid.newBuilder().setLsb(1L).setMsb(1L).build();
    public static final CommonTypes.Uuid OP_TWO_ACTIVE = CommonTypes.Uuid.newBuilder().setLsb(2L).setMsb(2L).build();
    public static final CommonTypes.Uuid OP_ALL_STANDBY = CommonTypes.Uuid.newBuilder().setLsb(3L).setMsb(3L).build();
    public static final CommonTypes.Uuid OP_INVALID = CommonTypes.Uuid.newBuilder().setLsb(4L).setMsb(4L).build();
    public static final CommonTypes.Uuid OP_ENFORCE_SNAPSHOT_FULL_SYNC = CommonTypes.Uuid.newBuilder().setLsb(5L).setMsb(5L).build();

    @Getter
    private long configId;

    @Getter
    private boolean shutdown;

    @Getter
    public ClusterManagerCallback clusterManagerCallback;

    private Thread thread;

    private CorfuRuntime corfuRuntime;

    private CorfuStore corfuStore;

    private ConfigStreamListener configStreamListener;

    public void start() {
        configId = 0L;
        shutdown = false;
        topologyConfig = constructTopologyConfigMsg();
        clusterManagerCallback = new ClusterManagerCallback(this);
        corfuRuntime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString("localhost:9000")
                .setTransactionLogging(true)
                .connect();
        corfuStore = new CorfuStore(corfuRuntime);
        CorfuStoreMetadata.Timestamp ts = corfuStore.getTimestamp();
        try {
            Table<Messages.Uuid, Messages.Uuid, Messages.Uuid> table = corfuStore.openTable(
                    CONFIG_NAMESPACE, CONFIG_TABLE_NAME,
                    Messages.Uuid.class, Messages.Uuid.class, Messages.Uuid.class,
                    TableOptions.builder().build()
            );
            table.clear();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        configStreamListener = new ConfigStreamListener(this);
        corfuStore.subscribe(configStreamListener, CONFIG_NAMESPACE,
                Collections.singletonList(new TableSchema(CONFIG_TABLE_NAME, CommonTypes.Uuid.class, CommonTypes.Uuid.class, CommonTypes.Uuid.class)), ts);
        thread = new Thread(clusterManagerCallback);
        thread.start();
    }

    @Override
    public void shutdown() {
        shutdown = true;
        if (corfuRuntime != null) {
            corfuRuntime.shutdown();
        }

        if(configStreamListener != null) {
            corfuStore.unsubscribe(configStreamListener);
        }

        log.info("Shutdown Cluster Manager completed.");
    }

    public static TopologyDescriptor readConfig() {
        ClusterDescriptor activeCluster;
        List<String> activeNodeNames = new ArrayList<>();
        List<String> standbyNodeNames = new ArrayList<>();
        List<String> activeNodeHosts = new ArrayList<>();
        List<String> standbyNodeHosts = new ArrayList<>();
        List<String> activeNodeIds = new ArrayList<>();
        List<String> standbyNodeIds = new ArrayList<>();
        String activeClusterId;
        String activeCorfuPort;
        String activeLogReplicationPort;

        String standbySiteName;
        String standbyCorfuPort;
        String standbyLogReplicationPort;

        File configFile = new File(CONFIG_FILE_PATH);
        try (FileReader reader = new FileReader(configFile)) {
            Properties props = new Properties();
            props.load(reader);

            Set<String> names = props.stringPropertyNames();

            activeClusterId = props.getProperty(ACTIVE_CLUSTER_NAME, DEFAULT_ACTIVE_CLUSTER_NAME);
            activeCorfuPort = props.getProperty(ACTIVE_CLUSTER_CORFU_PORT);
            activeLogReplicationPort = props.getProperty(LOG_REPLICATION_SERVICE_ACTIVE_PORT_NUM);
            for (int i = 0; i < NUM_NODES_PER_CLUSTER; i++) {
                String nodeName = ACTIVE_CLUSTER_NODE + i;
                if (!names.contains(nodeName)) {
                    continue;
                }
                activeNodeNames.add(nodeName);
                activeNodeHosts.add(props.getProperty(nodeName));
            }
            // TODO: add reading of node id (which is the APH node uuid)

            standbySiteName = props.getProperty(STANDBY_CLUSTER_NAME, DEFAULT_STANDBY_CLUSTER_NAME);
            standbyCorfuPort = props.getProperty(STANDBY_CLUSTER_CORFU_PORT);
            standbyLogReplicationPort = props.getProperty(LOG_REPLICATION_SERVICE_STANDBY_PORT_NUM);
            for (int i = 0; i < NUM_NODES_PER_CLUSTER; i++) {
                String nodeName = STANDBY_CLUSTER_NODE + i;
                if (!names.contains(nodeName)) {
                    continue;
                }
                standbyNodeNames.add(nodeName);
                standbyNodeHosts.add(props.getProperty(nodeName));
            }
            // TODO: add reading of node id (which is the APH node uuid)

        } catch (IOException e) {
            log.warn("Plugin Config File {} does not exist. Using default configs", CONFIG_FILE_PATH);
            activeClusterId = DefaultClusterConfig.getActiveClusterId();
            activeCorfuPort = DefaultClusterConfig.getActiveCorfuPort();
            activeLogReplicationPort = DefaultClusterConfig.getActiveLogReplicationPort();
            activeNodeNames.addAll(DefaultClusterConfig.getActiveNodeNames());
            activeNodeHosts.addAll(DefaultClusterConfig.getActiveIpAddresses());
            activeNodeIds.addAll(DefaultClusterConfig.getActiveNodesUuid());

            standbySiteName = DefaultClusterConfig.getStandbyClusterId();
            standbyCorfuPort = DefaultClusterConfig.getStandbyCorfuPort();
            standbyLogReplicationPort = DefaultClusterConfig.getStandbyLogReplicationPort();
            standbyNodeNames.addAll(DefaultClusterConfig.getActiveNodeNames());
            standbyNodeHosts.addAll(DefaultClusterConfig.getStandbyIpAddresses());
            standbyNodeIds.addAll(DefaultClusterConfig.getStandbyNodesUuid());
        }

        activeCluster = new ClusterDescriptor(activeClusterId, ClusterRole.ACTIVE, Integer.parseInt(activeCorfuPort));

        for (int i = 0; i < activeNodeNames.size(); i++) {
            log.info("Active Cluster Name {}, IpAddress {}", activeNodeNames.get(i), activeNodeHosts.get(i));
            NodeDescriptor nodeInfo = new NodeDescriptor(activeNodeHosts.get(i),
                    activeLogReplicationPort, ACTIVE_CLUSTER_NAME, UUID.fromString(activeNodeIds.get(i)));
            activeCluster.getNodesDescriptors().add(nodeInfo);
        }

        // Setup backup cluster information
        Map<String, ClusterDescriptor> standbySites = new HashMap<>();
        standbySites.put(STANDBY_CLUSTER_NAME, new ClusterDescriptor(standbySiteName, ClusterRole.STANDBY, Integer.parseInt(standbyCorfuPort)));

        for (int i = 0; i < standbyNodeNames.size(); i++) {
            log.info("Standby Cluster Name {}, IpAddress {}", standbyNodeNames.get(i), standbyNodeHosts.get(i));
            NodeDescriptor nodeInfo = new NodeDescriptor(standbyNodeHosts.get(i),
                    standbyLogReplicationPort, STANDBY_CLUSTER_NAME, UUID.fromString(standbyNodeIds.get(i)));
            standbySites.get(STANDBY_CLUSTER_NAME).getNodesDescriptors().add(nodeInfo);
        }

        log.info("Active Cluster Info {}; Standby Cluster Info {}", activeCluster, standbySites);
        return new TopologyDescriptor(0L, Arrays.asList(activeCluster), new ArrayList<>(standbySites.values()));
    }

    public static TopologyConfigurationMsg constructTopologyConfigMsg() {
        TopologyDescriptor clusterTopologyDescriptor = readConfig();
        return clusterTopologyDescriptor.convertToMessage();
    }


    @Override
    public TopologyConfigurationMsg queryTopologyConfig(boolean useCached) {
        if (topologyConfig == null || !useCached) {
            topologyConfig = constructTopologyConfigMsg();
        }

        log.debug("new cluster config msg " + topologyConfig);
        return topologyConfig;
    }

    /**
     * Create a new topology config, which changes one of the standby as the active,
     * and active as standby. Data should flow in the reverse direction.
     **/
    public TopologyDescriptor generateConfigWithRoleSwitch() {
        TopologyDescriptor currentConfig = new TopologyDescriptor(topologyConfig);

        List<ClusterDescriptor> newActiveClusters = new ArrayList<>();
        List<ClusterDescriptor> newStandbyClusters = new ArrayList<>();
        currentConfig.getActiveClusters().values().forEach(activeCluster ->
                newStandbyClusters.add(new ClusterDescriptor(activeCluster, ClusterRole.STANDBY)));
        for (ClusterDescriptor standbyCluster : currentConfig.getStandbyClusters().values()) {
            if (newActiveClusters.isEmpty()) {
                newActiveClusters.add(new ClusterDescriptor(standbyCluster, ClusterRole.ACTIVE));
            } else {
                newStandbyClusters.add(new ClusterDescriptor(standbyCluster, ClusterRole.STANDBY));
            }
        }

        return new TopologyDescriptor(++configId, newActiveClusters, newStandbyClusters);
    }

    /**
     * Create a new topology config, which marks all standby cluster as active on purpose.
     * System should drop messages between any two active clusters.
     **/
    public TopologyDescriptor generateConfigWithAllActive() {
        TopologyDescriptor currentConfig = new TopologyDescriptor(topologyConfig);
        ClusterDescriptor currentActive = currentConfig.getActiveClusters().values().iterator().next();

        List<ClusterDescriptor> newActiveClusters = new ArrayList<>();
        currentConfig.getStandbyClusters().values().forEach(standbyCluster ->
                newActiveClusters.add(new ClusterDescriptor(standbyCluster, ClusterRole.ACTIVE)));
        newActiveClusters.add(currentActive);

        return new TopologyDescriptor(++configId, newActiveClusters, new ArrayList<>());
    }

    /**
     * Create a new topology config, which marks all cluster as standby on purpose.
     * System should not send messages in this case.
     **/
    public TopologyDescriptor generateConfigWithAllStandby() {
        TopologyDescriptor currentConfig = new TopologyDescriptor(topologyConfig);
        ClusterDescriptor currentActive = currentConfig.getActiveClusters().values().iterator().next();

        List<ClusterDescriptor> newStandbyClusters = new ArrayList<>(currentConfig.getStandbyClusters().values());
        ClusterDescriptor newStandby = new ClusterDescriptor(currentActive, ClusterRole.STANDBY);
        newStandbyClusters.add(newStandby);

        return new TopologyDescriptor(++configId, new ArrayList<>(), newStandbyClusters);
    }

    /**
     * Create a new topology config, which marks all standby cluster as invalid on purpose.
     * System should not send messages in this case.
     **/
    public TopologyDescriptor generateConfigWithInvalid() {
        TopologyDescriptor currentConfig = new TopologyDescriptor(topologyConfig);

        List<ClusterDescriptor> newActiveClusters = new ArrayList<>(currentConfig.getActiveClusters().values());
        List<ClusterDescriptor> newInvalidClusters = new ArrayList<>();
        currentConfig.getStandbyClusters().values().forEach(standbyCluster ->
                newInvalidClusters.add(new ClusterDescriptor(standbyCluster, ClusterRole.INVALID)));

        return new TopologyDescriptor(++configId, newActiveClusters, new ArrayList<>(), newInvalidClusters);
    }

    /**
     * Bring topology config back to default valid config.
     **/
    public TopologyDescriptor generateDefaultValidConfig() {
        TopologyDescriptor defaultTopology = new TopologyDescriptor(constructTopologyConfigMsg());
        List<ClusterDescriptor> activeClusters = new ArrayList<>(defaultTopology.getActiveClusters().values());
        List<ClusterDescriptor> standbyClusters = new ArrayList<>(defaultTopology.getStandbyClusters().values());

        return new TopologyDescriptor(++configId, activeClusters, standbyClusters);
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
            log.info("ConfigStreamListener onNext {} with entry size {}", results, results.getEntries().size());
            CorfuStreamEntry entry = results.getEntries().values()
                    .stream()
                    .findFirst()
                    .map(corfuStreamEntries -> corfuStreamEntries.get(0))
                    .orElse(null);
            if (entry != null && entry.getOperation().equals(CorfuStreamEntry.OperationType.UPDATE)) {
                log.info("onNext entry is {}, key{}, p{}, m{}, op{}", entry, entry.getKey(), entry.getPayload(), entry.getMetadata(), entry.getOperation());
                if (entry.getKey().equals(OP_SWITCH)) {
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateConfigWithRoleSwitch());
                } else if (entry.getKey().equals(OP_TWO_ACTIVE)) {
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateConfigWithAllActive());
                } else if (entry.getKey().equals(OP_ALL_STANDBY)) {
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateConfigWithAllStandby());
                } else if (entry.getKey().equals(OP_INVALID)) {
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateConfigWithInvalid());
                } else if (entry.getKey().equals(OP_RESUME)) {
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateDefaultValidConfig());
                } else if(entry.getKey().equals(OP_ENFORCE_SNAPSHOT_FULL_SYNC)) {
                    try {
                        clusterManager.forceSnapshotSync(clusterManager.queryTopologyConfig(true).getClustersList().get(1).getId());
                    } catch (LogReplicationDiscoveryServiceException e) {
                        log.warn("Caught a RuntimeException ", e);
                        ClusterRole role = clusterManager.getCorfuReplicationDiscoveryService().getLocalClusterRoleType();
                        if (role != ClusterRole.STANDBY) {
                            log.error("The current cluster role is {} and should not throw a RuntimeException for forceSnapshotSync call.", role);
                            Thread.interrupted();
                        }
                    }
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            // Ignore
        }
    }

}
