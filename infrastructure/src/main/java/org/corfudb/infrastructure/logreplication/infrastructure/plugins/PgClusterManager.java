package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.PgUtils.PostgresConnector;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationDiscoveryServiceException;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.runtime.ExampleSchemas.ClusterUuidMsg;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PgClusterManager extends DefaultClusterManager {

    @Getter
    private long configId;

    @Getter
    private boolean shutdown;

    @Getter
    public PgClusterManagerCallback pgClusterManagerCallback;

    public static volatile boolean isTestEnvironment = false;

    public static final String PG_CONTAINER_PHYSICAL_HOST = "localhost";
    public static final String ACTIVE_CONTAINER_VIRTUAL_HOST = "postgres1";
    public static final String STANDBY_CONTAINER_VIRTUAL_HOST = "postgres2";

    public static final int ACTIVE_CONTAINER_PHYSICAL_PORT = 5442;
    public static final int STANDBY_CONTAINER_PHYSICAL_PORT = 5443;
    public static final int CONFIG_CONTAINER_PHYSICAL_PORT = 5444;

    public static final String TEST_PG_USER = "test";
    public static final String TEST_PG_PASSWORD = "test";
    public static final String TEST_PG_DATABASE = "test";

    public static final PostgresConnector configContainerPgConnector = new PostgresConnector(PG_CONTAINER_PHYSICAL_HOST,
            String.valueOf(CONFIG_CONTAINER_PHYSICAL_PORT), TEST_PG_USER, TEST_PG_PASSWORD, TEST_PG_DATABASE);

    private static final List<String> activeNodeNames = Collections.singletonList(ACTIVE_CONTAINER_VIRTUAL_HOST);
    private static final List<String> standbyNodeNameAddress = Collections.singletonList(STANDBY_CONTAINER_VIRTUAL_HOST);

    private ScheduledExecutorService scheduler;

    private final Set<String> processedTopologyIds = new HashSet<>();

    public void start() {
        configId = 0L;
        shutdown = false;
        topologyConfig = constructTopologyConfigMsg();
        pgClusterManagerCallback = new PgClusterManagerCallback(this);

        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::pollConfigTable, 0, 5, TimeUnit.SECONDS);

        Thread thread = new Thread(pgClusterManagerCallback);
        thread.start();
    }

    public static TopologyDescriptor readConfig() {
        ClusterDescriptor activeCluster;
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


        activeClusterId = DefaultClusterConfig.getActiveClusterId();
        activeCorfuPort = String.valueOf(ACTIVE_CONTAINER_PHYSICAL_PORT);
        activeLogReplicationPort = activeCorfuPort;
        activeNodeHosts.addAll(activeNodeNames);
        activeNodeIds.addAll(DefaultClusterConfig.getActiveNodesUuid());

        standbySiteName = DefaultClusterConfig.getStandbyClusterId();
        standbyCorfuPort = String.valueOf(STANDBY_CONTAINER_PHYSICAL_PORT);
        standbyLogReplicationPort = standbyCorfuPort;
        standbyNodeNames.addAll(DefaultClusterConfig.getActiveNodeNames());
        standbyNodeHosts.addAll(standbyNodeNameAddress);
        standbyNodeIds.addAll(DefaultClusterConfig.getStandbyNodesUuid());

        activeCluster = new ClusterDescriptor(activeClusterId, ClusterRole.ACTIVE, Integer.parseInt(activeCorfuPort));

        for (int i = 0; i < activeNodeNames.size(); i++) {
            log.info("Active Cluster Name {}, IpAddress {}", activeNodeNames.get(i), activeNodeHosts.get(i));
            NodeDescriptor nodeInfo = new NodeDescriptor(activeNodeHosts.get(i),
                    activeLogReplicationPort, ACTIVE_CLUSTER_NAME, activeNodeIds.get(i), activeNodeIds.get(i));
            activeCluster.getNodesDescriptors().add(nodeInfo);
        }

        // Setup backup cluster information
        Map<String, ClusterDescriptor> standbySites = new HashMap<>();
        standbySites.put(STANDBY_CLUSTER_NAME, new ClusterDescriptor(standbySiteName, ClusterRole.STANDBY, Integer.parseInt(standbyCorfuPort)));

        for (int i = 0; i < standbyNodeNames.size(); i++) {
            log.info("Standby Cluster Name {}, IpAddress {}", standbyNodeNames.get(i), standbyNodeHosts.get(i));
            NodeDescriptor nodeInfo = new NodeDescriptor(standbyNodeHosts.get(i),
                    standbyLogReplicationPort, STANDBY_CLUSTER_NAME, standbyNodeIds.get(i), standbyNodeIds.get(i));
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
     * Create a new topology config, which replaces the active cluster with a backup cluster.
     **/
    public TopologyDescriptor generateConfigWithBackup() {
        TopologyDescriptor currentConfig = new TopologyDescriptor(topologyConfig);
        ClusterDescriptor currentActive = currentConfig.getActiveClusters().values().iterator().next();

        List<ClusterDescriptor> newActiveClusters = new ArrayList<>();
        newActiveClusters.add(new ClusterDescriptor(
                currentActive.getClusterId(), ClusterRole.ACTIVE, BACKUP_CORFU_PORT));

        NodeDescriptor backupNode = new NodeDescriptor(
                DefaultClusterConfig.getDefaultHost(),
                DefaultClusterConfig.getBackupLogReplicationPort(),
                BACKUP_CLUSTER_NAME,
                DefaultClusterConfig.getBackupNodesUuid().get(0),
                DefaultClusterConfig.getBackupNodesUuid().get(0)
        );
        newActiveClusters.get(0).getNodesDescriptors().add(backupNode);
        List<ClusterDescriptor> standbyClusters = new ArrayList<>(currentConfig.getStandbyClusters().values());

        return new TopologyDescriptor(++configId, newActiveClusters, standbyClusters);
    }



    /**
     * Testing purpose to generate cluster role change.
     */
    public static class PgClusterManagerCallback implements Runnable {
        private final PgClusterManager clusterManager;
        private final LinkedBlockingQueue<TopologyDescriptor> queue;

        public PgClusterManagerCallback(PgClusterManager clusterManager) {
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

    @Override
    public void shutdown() {
        shutdown = true;
        if (scheduler != null) {
            scheduler.shutdown();
        }
        super.shutdown();
    }

    private void pollConfigTable() {

        String query = "SELECT config_key, config_value FROM topology_config ORDER BY id DESC LIMIT 1;";

        List<Map<String, Object>> result = new ArrayList<>();
        log.info("Executing command: {}, on connector {} ", query, configContainerPgConnector);

        try (Connection conn = DriverManager.getConnection(configContainerPgConnector.url, configContainerPgConnector.user, configContainerPgConnector.password)) {
            Statement statement = conn.createStatement();
            ResultSet results = statement.executeQuery(query);
            ResultSetMetaData metaData = results.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (results.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), results.getObject(i));
                }
                result.add(row);
            }

            statement.close();

            if (!result.isEmpty()) {
                Map<String, Object> row = result.get(0);
                if(!processedTopologyIds.contains((String) row.get("id"))) {
                    processedTopologyIds.add((String) row.get("id"));
                    String configKey = (String) row.get("config_key");
                    String configValue = (String) row.get("config_value");

                    if ("TOPOLOGY_CHANGE".equals(configKey)) {
                        applyTopologyChange(configValue);
                    }
                }
            }
        } catch (SQLException e) {
            log.info("ERROR", e);
        }
    }

    private void applyTopologyChange(String change) {
        ClusterUuidMsg msg;
        switch (change) {
            case "SWITCH":
                msg = OP_SWITCH;
                break;
            case "TWO_ACTIVE":
                msg = OP_TWO_ACTIVE;
                break;
            case "ALL_STANDBY":
                msg = OP_ALL_STANDBY;
                break;
            case "INVALID":
                msg = OP_INVALID;
                break;
            case "RESUME":
                msg = OP_RESUME;
                break;
            case "ENFORCE_SNAPSHOT_FULL_SYNC":
                msg = OP_ENFORCE_SNAPSHOT_FULL_SYNC;
                break;
            case "BACKUP":
                msg = OP_BACKUP;
                break;
            default:
                log.warn("Unknown topology change: {}", change);
                return;
        }

        processTopologyChange(msg);
    }

    private void processTopologyChange(ClusterUuidMsg msg) {
        log.info("Processing topology change: {}", msg);
        if (msg.equals(OP_SWITCH)) {
            pgClusterManagerCallback.applyNewTopologyConfig(generateConfigWithRoleSwitch());
        } else if (msg.equals(OP_TWO_ACTIVE)) {
            pgClusterManagerCallback.applyNewTopologyConfig(generateConfigWithAllActive());
        } else if (msg.equals(OP_ALL_STANDBY)) {
            pgClusterManagerCallback.applyNewTopologyConfig(generateConfigWithAllStandby());
        } else if (msg.equals(OP_INVALID)) {
            pgClusterManagerCallback.applyNewTopologyConfig(generateConfigWithInvalid());
        } else if (msg.equals(OP_RESUME)) {
            pgClusterManagerCallback.applyNewTopologyConfig(generateDefaultValidConfig());
        } else if (msg.equals(OP_ENFORCE_SNAPSHOT_FULL_SYNC)) {
            try {
                forceSnapshotSync(queryTopologyConfig(true).getClustersList().get(1).getId());
            } catch (LogReplicationDiscoveryServiceException e) {
                log.warn("Caught a RuntimeException ", e);
                ClusterRole role = getCorfuReplicationDiscoveryService().getLocalClusterRoleType();
                if (role != ClusterRole.STANDBY) {
                    log.error("The current cluster role is {} and should not throw a RuntimeException for forceSnapshotSync call.", role);
                    Thread.currentThread().interrupt();
                }
            }
        } else if (msg.equals(OP_BACKUP)) {
            pgClusterManagerCallback.applyNewTopologyConfig(generateConfigWithBackup());
        }
    }
}