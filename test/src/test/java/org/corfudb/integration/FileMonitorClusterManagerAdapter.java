package org.corfudb.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.CorfuReplicationClusterManagerAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class represents a Cluster Manager Adapter used for testing purposes which
 * reads topology configuration from a file and triggers cluster change when
 * a parameter is changed on this monitored file, indicating cluster role change/flip to occur.
 *
 * @author amartinezman
 */
@Slf4j
public class FileMonitorClusterManagerAdapter extends CorfuReplicationClusterManagerAdapter {

    public final static int MONITOR_INTERVAL = 100;

    public final static String TOPOLOGY_FILE = "src/test/resources/topology-test.json";

    private final static int COMPLETE_PERCENTAGE = 100;

    private ScheduledExecutorService fileMonitor;

    private long topologyConfigId;

    private TopologyDescriptor topology;

    private ScheduledFuture<?> monitorFuture;

    private String topologyFileAbsolutePath;

    public FileMonitorClusterManagerAdapter() {
        File topologyFile = new File(TOPOLOGY_FILE);
        this.topologyFileAbsolutePath = topologyFile.getAbsolutePath();
    }

    //region Cluster Manager Adapter Override Methods

    @Override
    public TopologyConfigurationMsg queryTopologyConfig() {
        try {
            // Return topology read from file
            topology = readTopologyFromJSONFile(topologyFileAbsolutePath);
            topologyConfig = topology.convertToMessage();
            topologyConfigId = topology.getTopologyConfigId();
        } catch (Exception e) {
            log.error("Caught exception while querying topology", e);
        }

        return topologyConfig;
    }

    @Override
    public void start() {
        log.info("Start cluster manager adapter. Reading from file no connection to establish.");

        // Start a daemon thread which will monitor this file for cluster role change signal
        fileMonitor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("ClusterManagerMonitor")
                .build());

        monitorFuture = fileMonitor.scheduleAtFixedRate(this::monitor, 0L, MONITOR_INTERVAL, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        log.info("Shutdown cluster manager adapter. We are reading from a file no connection to be broken.");

        fileMonitor.shutdownNow();
    }

    //endregion

    /**
     * Monitor a File for changes, changes will indicate a topology change.
     */
    public void monitor() {
        JSONParser parser = new JSONParser();
        try {
            Object object = parser.parse(new FileReader(topologyFileAbsolutePath));

            JSONObject topology = (JSONObject) object;
            long newTopologyConfigId = (long) topology.get("topologyConfigId");

            if (newTopologyConfigId > topologyConfigId) {
                log.info("***** Cluster Role Change :: prev={} new={}", topologyConfigId, newTopologyConfigId);
                topologyConfigId = newTopologyConfigId;

                // Trigger cluster role change based on topology config id change
                log.info("***** Trigger Cluster Role Change");
                getCorfuReplicationDiscoveryService().prepareClusterRoleChange();

                cancelAndReSchedule();
            }
        } catch (Exception e) {
            // Retry
        }
    }

    private void cancelScheduler() {
        monitorFuture.cancel(true);
    }

    private void cancelAndReSchedule() {
        cancelScheduler();
        monitorFuture = fileMonitor.scheduleAtFixedRate(this::monitorStatus, 0L, MONITOR_INTERVAL, TimeUnit.MILLISECONDS);
    }

    /**
     * Load topology from a JSON File.
     *
     * @param topologyFile path to file holding the topology in JSON format
     * @return topology descriptor
     *
     * @throws Exception
     */
    public static TopologyDescriptor readTopologyFromJSONFile(String topologyFile) throws Exception {
        JSONParser parser = new JSONParser();
        FileReader fileReader = new FileReader(topologyFile);

        try {
            Object object = parser.parse(fileReader);

            // Topology as JSON
            JSONObject topologyObject = (JSONObject) object;
            long currentTopologyConfigId = (long) topologyObject.get("topologyConfigId");
            Map activeClusters = ((Map)topologyObject.get("activeClusters"));
            Map standbyClusters = ((Map)topologyObject.get("standbyClusters"));

            // Active Clusters
            List<ClusterDescriptor> actives = new ArrayList<>();
            Iterator itA = activeClusters.values().iterator();
            while (itA.hasNext()) {
                Map standbyClusterMap = (Map)itA.next();
                actives.add(getClusterDescriptor(standbyClusterMap));
            }

            // Standby Clusters
            List<ClusterDescriptor> standbys = new ArrayList<>();
            Iterator itS = standbyClusters.values().iterator();
            while (itS.hasNext()) {
                Map standbyClusterMap = (Map)itS.next();
                standbys.add(getClusterDescriptor(standbyClusterMap));
            }

            TopologyDescriptor topologyDescriptor = new TopologyDescriptor(currentTopologyConfigId, actives, standbys);

            log.info("Topology read from file :: {}", topologyDescriptor);

            return topologyDescriptor;

        } catch (Exception e) {
            log.error("Exception caught while reading topology...", e);
            e.printStackTrace();
            throw e;
        } finally {
            if (fileReader != null) {
                fileReader.close();
            }
        }
    }

    public void writeTopologyToJSONFile(TopologyDescriptor topology, String topologyFile) {

        ObjectMapper mapper = new ObjectMapper();

        File file = new File(topologyFile);
        try {
            // Serialize Java object info JSON file.
            mapper.writeValue(file, topology);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ClusterDescriptor getClusterDescriptor(Map cluster) {
        // Cluster Descriptor
        String clusterId = (String) cluster.get("clusterId");
        LogReplicationClusterInfo.ClusterRole role = LogReplicationClusterInfo.ClusterRole.valueOf((String)cluster.get("role"));
        long corfuPort = (Long)cluster.get("corfuPort");

        ClusterDescriptor clusterDescriptor = new ClusterDescriptor(clusterId, role, (int)corfuPort);

        // Getting Node Descriptors (List)
        JSONArray nodes = (JSONArray) cluster.get("nodesDescriptors");
        Iterator nodesIterator = nodes.iterator();

        while (nodesIterator.hasNext()) {
            Map node = ((Map)nodesIterator.next());
            String host = (String) node.get("host");
            String port = (String) node.get("port");
            UUID nodeId = UUID.fromString((String) node.get("nodeId"));

            clusterDescriptor.addNode(new NodeDescriptor(host, port, clusterId, nodeId));
        }

        return clusterDescriptor;
    }

    public void monitorStatus() {
        int replicationStatus = getCorfuReplicationDiscoveryService().queryReplicationStatus();
        log.info("Replication status {}", replicationStatus);

        if (replicationStatus == COMPLETE_PERCENTAGE) {
            // Log Replication is complete, this site may become standby and remote site active
            log.info("Cluster Role Change, this node will soon become STANDBY.");
            ClusterDescriptor newStandbyCluster = new ClusterDescriptor(topology.getActiveClusters().values().iterator().next(),
                    LogReplicationClusterInfo.ClusterRole.STANDBY);
            ClusterDescriptor newActiveCluster = new ClusterDescriptor(topology.getStandbyClusters().values().iterator().next(),
                    LogReplicationClusterInfo.ClusterRole.ACTIVE);
            TopologyDescriptor newTopology = new TopologyDescriptor(topologyConfigId, newActiveCluster,
                    Arrays.asList(newStandbyCluster));

            try {
                writeTopologyToJSONFile(newTopology, topologyFileAbsolutePath);
                updateTopologyConfig(newTopology.convertToMessage());
                cancelScheduler();
            } catch (Exception e) {
                log.error("Failed to enforce cluster role change");
                throw new UnrecoverableCorfuError("Failed to enforce cluster role change");
            }
        } else if(replicationStatus == -1) {
            log.info("Wait on standby to become ACTIVE");
            try {
                // If it's the Standby Cluster, Inspect Cluster Descriptor until it detects that it has become active
                TopologyDescriptor newTopology = readTopologyFromJSONFile(topologyFileAbsolutePath);
                log.info("New Topology {}", newTopology);
                log.info("Former Topology {}", topology);
                if (newTopology.getActiveClusters().values().iterator().next().getClusterId().equals(
                        topology.getStandbyClusters().values().iterator().next().getClusterId())) {
                    log.info("Standby has become ACTIVE");
                    updateTopologyConfig(newTopology.convertToMessage());
                }
            } catch (Exception e) {
                log.error("Caught exception while waiting for transition from standby to active", e);
            }
        }
    }

    public void monitorAnyChange() {
        File file = new File(topologyFileAbsolutePath);
        final Path path = FileSystems.getDefault().getPath(file.getParentFile().toString());
        try (final WatchService watchService = FileSystems.getDefault().newWatchService()) {
            final WatchKey watchKey = path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            while (true) {
                final WatchKey wk = watchService.take();
                for (WatchEvent<?> event : wk.pollEvents()) {
                    //we only register "ENTRY_MODIFY" so the context is always a Path.
                    final Path changed = (Path) event.context();
                    System.out.println(changed);
                    if (changed.endsWith("topology-test.json")) {
                        // Trigger cluster role change based on topology config id change
                        System.out.println("######## OMGGGGGGGGGG");
                        log.info("Trigger Cluster Role Change");
                        getCorfuReplicationDiscoveryService().prepareClusterRoleChange();
                        return;
                    }
                }
                // reset the key
                boolean valid = wk.reset();
                if (!valid) {
                    System.out.println("Key has been unregistered");
                }
            }
        } catch (Exception e) {
            //
        }
    }
}

