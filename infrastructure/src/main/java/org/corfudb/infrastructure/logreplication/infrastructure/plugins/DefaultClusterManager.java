package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.ExampleSchemas.ClusterUuidMsg;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Address;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.corfudb.common.util.URLUtils.getVersionFormattedHostAddress;

/**
 * This class extends CorfuReplicationClusterManagerAdapter, provides topology config API
 * for integration tests. The initial topology config should be valid, which means it has only
 * one active cluster, and one or more standby clusters.
 */
@Slf4j
public class DefaultClusterManager extends CorfuReplicationClusterManagerBaseAdapter {
    private static final int BACKUP_CORFU_PORT = 9007;

    private static final String ACTIVE_CLUSTER_NAME = "primary_site";
    private static final String STANDBY_CLUSTER_NAME = "standby_site";
    private static final String BACKUP_CLUSTER_NAME = "backup_site";

    public static final String CONFIG_NAMESPACE = "ns_lr_config_it";
    public static final String CONFIG_TABLE_NAME = "lr_config_it";
    public static final ClusterUuidMsg OP_RESUME = ClusterUuidMsg.newBuilder().setLsb(0L).setMsb(0L).build();
    public static final ClusterUuidMsg OP_SWITCH = ClusterUuidMsg.newBuilder().setLsb(1L).setMsb(1L).build();
    public static final ClusterUuidMsg OP_TWO_ACTIVE = ClusterUuidMsg.newBuilder().setLsb(2L).setMsb(2L).build();
    public static final ClusterUuidMsg OP_ALL_STANDBY = ClusterUuidMsg.newBuilder().setLsb(3L).setMsb(3L).build();
    public static final ClusterUuidMsg OP_INVALID = ClusterUuidMsg.newBuilder().setLsb(4L).setMsb(4L).build();
    public static final ClusterUuidMsg OP_ENFORCE_SNAPSHOT_FULL_SYNC = ClusterUuidMsg.newBuilder().setLsb(5L).setMsb(5L).build();
    public static final ClusterUuidMsg OP_BACKUP = ClusterUuidMsg.newBuilder().setLsb(6L).setMsb(6L).build();

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

    public DefaultClusterManager() {
        topology = new DefaultClusterConfig();
    }

    public void start() {
        configId = 0L;
        shutdown = false;
        topologyConfig = constructTopologyConfigMsg();
        clusterManagerCallback = new ClusterManagerCallback(this);
        corfuRuntime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(corfuEndpoint)
                .connect();
        corfuStore = new CorfuStore(corfuRuntime);
        long trimMark = Address.NON_ADDRESS;
        try {
            // Subscribe from the earliest point in the log.
            trimMark = corfuRuntime.getLayoutView().getRuntimeLayout().getLogUnitClient(corfuRuntime.getLayoutServers().get(0)).getTrimMark().get();
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

    public TopologyDescriptor readConfig() {
        List<ClusterDescriptor> activeClusters = new ArrayList<>();
        List<ClusterDescriptor> standbyClusters = new ArrayList<>();

        List<String> activeClusterIds = topology.getActiveClusterIds();
        List<String> activeCorfuPorts = topology.getActiveCorfuPorts();
        List<String> activeLogReplicationPorts =
            topology.getActiveLogReplicationPorts();
        List<String> activeNodeNames = topology.getActiveNodeNames();
        List<String> activeNodeHosts = topology.getActiveIpAddresses();
        List<String> activeNodeIds = topology.getActiveNodeUuids();

        List<String> standbyClusterIds = topology.getStandbyClusterIds();
        List<String> standbyCorfuPorts = topology.getStandbyCorfuPorts();
        List<String> standbyLogReplicationPorts =
            topology.getStandbyLogReplicationPorts();
        List<String> standbyNodeNames = topology.getActiveNodeNames();
        List<String> standbyNodeHosts = topology.getStandbyIpAddresses();
        List<String> standbyNodeIds = topology.getStandbyNodeUuids();

        // Setup active cluster information
        for (int i = 0; i < activeClusterIds.size(); i++) {
            ClusterDescriptor activeCluster = new ClusterDescriptor(
                activeClusterIds.get(i), ClusterRole.ACTIVE,
                Integer.parseInt(activeCorfuPorts.get(i)));

            for (int j = 0; j < activeNodeNames.size(); j++) {
                log.info("Active Cluster Name {}, IpAddress {}",
                    activeNodeNames.get(j), activeNodeHosts.get(i));
                NodeDescriptor nodeInfo =
                    new NodeDescriptor(activeNodeHosts.get(i),
                        activeLogReplicationPorts.get(i), ACTIVE_CLUSTER_NAME,
                        activeNodeIds.get(i), activeNodeIds.get(i));
                activeCluster.getNodesDescriptors().add(nodeInfo);
            }
            activeClusters.add(activeCluster);
        }

        // Setup standby cluster information
        for (int i = 0; i < standbyClusterIds.size(); i++) {
            ClusterDescriptor standbyCluster = new ClusterDescriptor(
                standbyClusterIds.get(i), ClusterRole.STANDBY,
                Integer.parseInt(standbyCorfuPorts.get(i)));

            for (int j = 0; j < standbyNodeNames.size(); j++) {
                log.info("Standby Cluster Name {}, IpAddress {}",
                    standbyNodeNames.get(j), standbyNodeHosts.get(i));
                NodeDescriptor nodeInfo =
                    new NodeDescriptor(standbyNodeHosts.get(i),
                        standbyLogReplicationPorts.get(i), STANDBY_CLUSTER_NAME,
                        standbyNodeIds.get(i), standbyNodeIds.get(i));
                standbyCluster.getNodesDescriptors().add(nodeInfo);
            }
            standbyClusters.add(standbyCluster);
        }

        return new TopologyDescriptor(0L, activeClusters,
            standbyClusters);
    }

    private TopologyConfigurationMsg constructTopologyConfigMsg() {
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

        currentConfig.getStandbyClusters().values().forEach(standbyCluster ->
            newActiveClusters.add(new ClusterDescriptor(standbyCluster, ClusterRole.ACTIVE)));

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
     * LR should not replicate to these clusters.
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
     * Create a new topology config, which replaces the active cluster with a backup cluster.
     **/
    public TopologyDescriptor generateConfigWithBackup() {
        TopologyDescriptor currentConfig = new TopologyDescriptor(topologyConfig);
        Optional<ClusterDescriptor> currentActive =
            currentConfig.getActiveClusters()
            .values().stream().filter(cluster -> cluster.getClusterId()
                .equals(topology.getActiveClusterIds().get(0))).findFirst();

        List<ClusterDescriptor> newActiveClusters = new ArrayList<>();
        newActiveClusters.add(new ClusterDescriptor(
            currentActive.get().getClusterId(), ClusterRole.ACTIVE,
            BACKUP_CORFU_PORT));

        NodeDescriptor backupNode = new NodeDescriptor(
                getVersionFormattedHostAddress(topology.getDefaultHost()),
                topology.getBackupLogReplicationPort(),
                BACKUP_CLUSTER_NAME,
                topology.getBackupNodesUuid().get(0),
                topology.getBackupNodesUuid().get(0)
                );
        newActiveClusters.get(0).getNodesDescriptors().add(backupNode);
        List<ClusterDescriptor> standbyClusters =
            new ArrayList<>(currentConfig.getStandbyClusters().values());

        return new TopologyDescriptor(++configId, newActiveClusters, standbyClusters);
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
            log.info("onNext :: entry size={}", results.getEntries().size());
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
                } else if (entry.getKey().equals(OP_ENFORCE_SNAPSHOT_FULL_SYNC)) {
                    try {
                        // Enforce snapshot sync on the 1st standby cluster
                        List<ClusterConfigurationMsg> clusters =
                            clusterManager.queryTopologyConfig(true).getClustersList();
                        Optional<ClusterConfigurationMsg> standbyCluster =
                            clusters.stream().filter(cluster -> cluster.getRole() == ClusterRole.STANDBY
                            && cluster.getId().equals(clusterManager.topology.getStandbyClusterIds().get(0))).findFirst();
                        clusterManager.forceSnapshotSync(
                            standbyCluster.get().getId());
                    } catch (LogReplicationDiscoveryServiceException e) {
                        log.warn("Caught a RuntimeException ", e);
                        ClusterRole role = clusterManager.getCorfuReplicationDiscoveryService().getLocalClusterRoleType();
                        if (role != ClusterRole.STANDBY) {
                            log.error("The current cluster role is {} and should not throw a RuntimeException for forceSnapshotSync call.", role);
                            Thread.interrupted();
                        }
                    }
                } else if (entry.getKey().equals(OP_BACKUP)) {
                    clusterManager.getClusterManagerCallback()
                            .applyNewTopologyConfig(clusterManager.generateConfigWithBackup());
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
