package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static java.lang.Thread.sleep;

@Slf4j
public class DefaultClusterManager extends CorfuReplicationClusterManagerBaseAdapter {
    public static long epoch = 0;
    public static final int changeInterval = 5000;
    public static final String config_file = "/config/corfu/corfu_replication_config.properties";
    public static final String DEFAULT_ACTIVE_CLUSTER_NAME = "primary_site";
    public static final String DEFAULT_STANDBY_CLUSTER_NAME = "standby_site";
    private static final int NUM_NODES_PER_CLUSTER = 3;

    private static final String ACTIVE_CLUSTER_NAME = "primary_site";
    private static final String STANDBY_CLUSTER_NAME = "standby_site";
    private static final String ACTIVE_CLUSTER_CORFU_PORT = "primary_site_corfu_portnumber";
    private static final String STANDBY_CLUSTER_CORFU_PORT = "standby_site_corfu_portnumber";
    private static final String LOG_REPLICATION_SERVICE_ACTIVE_PORT_NUM = "primary_site_portnumber";
    private static final String LOG_REPLICATION_SERVICE_STANDBY_PORT_NUM = "standby_site_portnumber";

    private static final String ACTIVE_CLUSTER_NODE = "primary_site_node";
    private static final String STANDBY_CLUSTER_NODE = "standby_site_node";
    private boolean ifShutdown = false;

    @Getter
    public SiteManagerCallback siteManagerCallback;

    private Thread thread;



    public void start() {
        siteManagerCallback = new SiteManagerCallback(this);
        thread = new Thread(siteManagerCallback);
        thread.start();
    }

    @Override
    public void shutdown() {
        ifShutdown = true;
    }


    public static TopologyDescriptor readConfig() throws IOException {
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

        File configFile = new File(config_file);
        try {
            FileReader reader = new FileReader(configFile);
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

            reader.close();
        } catch (FileNotFoundException e) {
            log.warn("Plugin Config File {} does not exist. Using default configs", config_file);
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
        TopologyDescriptor clusterTopologyDescriptor = null;
        TopologyConfigurationMsg clusterConfigurationMsg;

        try {
            clusterTopologyDescriptor = readConfig();
        } catch (Exception e) {
            log.warn("Caught an exception while loading Topology descriptor " + e);
        }

        clusterConfigurationMsg = clusterTopologyDescriptor.convertToMessage();
        return clusterConfigurationMsg;
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
     * Enforce one of the standby Cluster's to become the new active cluster and current active to become standby
     **/
    public static TopologyDescriptor changeActiveCluster(TopologyConfigurationMsg topologyConfig) {
        TopologyDescriptor topologyDescriptor = new TopologyDescriptor(topologyConfig);

        // Convert the current active to standby
        ClusterDescriptor oldActive = topologyDescriptor.getActiveClusters().values().iterator().next();
        ClusterDescriptor newStandby = new ClusterDescriptor(oldActive, ClusterRole.STANDBY);

        List<ClusterDescriptor> standbyClusters = Arrays.asList(newStandby);
        ClusterDescriptor newPrimary = null;

        for (ClusterDescriptor standbyCluster : topologyDescriptor.getStandbyClusters().values()) {
            if (newPrimary == null) {
                newPrimary = new ClusterDescriptor(standbyCluster, ClusterRole.ACTIVE);
            } else {
                standbyClusters.add(standbyCluster);
            }
        }

        TopologyDescriptor newSiteConf = new TopologyDescriptor(1L, Arrays.asList(newPrimary), standbyClusters);
        return newSiteConf;
    }

    /**
     * Testing purpose to generate cluster role change.
     */
    public static class SiteManagerCallback implements Runnable {
        public boolean clusterRoleChange = false;
        DefaultClusterManager clusterManager;

        SiteManagerCallback(DefaultClusterManager clusterManager) {
            this.clusterManager = clusterManager;
        }

        @Override
        public void run() {
            while (!clusterManager.ifShutdown) {
                try {
                    sleep(changeInterval);
                    if (clusterRoleChange) {
                        TopologyDescriptor newConfig = changeActiveCluster(clusterManager.getTopologyConfig());
                        clusterManager.updateTopologyConfig(newConfig.convertToMessage());
                        log.warn("Change the cluster config");
                        clusterRoleChange = false;
                    }
                } catch (Exception e) {
                    log.error("Caught an exception",e);
                }
            }
        }
    }

}
