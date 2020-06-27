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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static java.lang.Thread.sleep;

@Slf4j
public class DefaultClusterManager extends CorfuReplicationClusterManagerAdapter {
    public static long epoch = 0;
    public static final int changeInterval = 5000;
    public static final String config_file = "/config/corfu/corfu_replication_config.properties";
    private static final String DEFAULT_PRIMARY_SITE_NAME = "primary_site";
    private static final String DEFAULT_STANDBY_SITE_NAME = "standby_site";
    private static final int NUM_NODES_PER_CLUSTER = 3;

    private static final String PRIMARY_SITE_NAME = "primary_site";
    private static final String STANDBY_SITE_NAME = "standby_site";
    private static final String PRIMARY_SITE_CORFU_PORT = "primary_site_corfu_portnumber";
    private static final String STANDBY_SITE_CORFU_PORT = "standby_site_corfu_portnumber";
    private static final String LOG_REPLICATION_SERVICE_PRIMARY_PORT_NUM = "primary_site_portnumber";
    private static final String LOG_REPLICATION_SERVICE_STANDBY_PORT_NUM = "standby_site_portnumber";
    private static final String PRIMARY_SITE_NODEID = "primary_site_node_id";
    private static final String STANDBY_SITE_NODEID = "standby_site_node_id";

    private static final String PRIMARY_SITE_NODE = "primary_site_node";
    private static final String STANDBY_SITE_NODE = "standby_site_node";
    private boolean ifShutdown = false;

    @Getter
    public SiteManagerCallback siteManagerCallback;

    Thread thread = new Thread(siteManagerCallback);

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
        ClusterDescriptor primarySite;
        List<String> primaryNodeNames = new ArrayList<>();
        List<String> standbyNodeNames = new ArrayList<>();
        List<String> primaryIpAddresses = new ArrayList<>();
        List<String> standbyIpAddresses = new ArrayList<>();
        List<String> primaryNodeIds = new ArrayList<>();
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

            activeClusterId = props.getProperty(PRIMARY_SITE_NAME, DEFAULT_PRIMARY_SITE_NAME);
            activeCorfuPort = props.getProperty(PRIMARY_SITE_CORFU_PORT);
            activeLogReplicationPort = props.getProperty(LOG_REPLICATION_SERVICE_PRIMARY_PORT_NUM);
            for (int i = 0; i < NUM_NODES_PER_CLUSTER; i++) {
                String nodeName = PRIMARY_SITE_NODE + i;
                if (!names.contains(nodeName)) {
                    continue;
                }
                primaryNodeNames.add(nodeName);
                primaryIpAddresses.add(props.getProperty(nodeName));
            }
            // TODO: add reading of node id (which is the APH node uuid)

            standbySiteName = props.getProperty(STANDBY_SITE_NAME, DEFAULT_STANDBY_SITE_NAME);
            standbyCorfuPort = props.getProperty(STANDBY_SITE_CORFU_PORT);
            standbyLogReplicationPort = props.getProperty(LOG_REPLICATION_SERVICE_STANDBY_PORT_NUM);
            for (int i = 0; i < NUM_NODES_PER_CLUSTER; i++) {
                String nodeName = STANDBY_SITE_NODE + i;
                if (!names.contains(nodeName)) {
                    continue;
                }
                standbyNodeNames.add(nodeName);
                standbyIpAddresses.add(props.getProperty(nodeName));
            }
            // TODO: add reading of node id (which is the APH node uuid)

            reader.close();
        } catch (FileNotFoundException e) {
            log.warn("Site Config File {} does not exist.  Using default configs", config_file);
            activeClusterId = DefaultClusterConfig.getActiveClusterId();
            activeCorfuPort = DefaultClusterConfig.getActiveCorfuPort();
            activeLogReplicationPort = DefaultClusterConfig.getActiveLogReplicationPort();
            primaryNodeNames.addAll(DefaultClusterConfig.getActiveNodeNames());
            primaryIpAddresses.addAll(DefaultClusterConfig.getActiveIpAddresses());
            primaryNodeIds.addAll(DefaultClusterConfig.getActiveNodesUuid());

            standbySiteName = DefaultClusterConfig.getStandbyClusterId();
            standbyCorfuPort = DefaultClusterConfig.getStandbyCorfuPort();
            standbyLogReplicationPort = DefaultClusterConfig.getStandbyLogReplicationPort();
            standbyNodeNames.addAll(DefaultClusterConfig.getActiveNodeNames());
            standbyIpAddresses.addAll(DefaultClusterConfig.getStandbyIpAddresses());
            standbyNodeIds.addAll(DefaultClusterConfig.getStandbyNodesUuid());
        }

        primarySite = new ClusterDescriptor(activeClusterId, ClusterRole.ACTIVE, Integer.parseInt(activeCorfuPort));

        for (int i = 0; i < primaryNodeNames.size(); i++) {
            log.info("Primary Site Name {}, IpAddress {}", primaryNodeNames.get(i), primaryIpAddresses.get(i));
            NodeDescriptor nodeInfo = new NodeDescriptor(primaryIpAddresses.get(i),
                    activeLogReplicationPort, ClusterRole.ACTIVE, PRIMARY_SITE_NAME, UUID.fromString(primaryNodeIds.get(i)));
            primarySite.getNodesDescriptors().add(nodeInfo);
        }

        // Setup backup cluster information
        Map<String, ClusterDescriptor> standbySites = new HashMap<>();
        standbySites.put(STANDBY_SITE_NAME, new ClusterDescriptor(standbySiteName, ClusterRole.STANDBY, Integer.parseInt(standbyCorfuPort)));

        for (int i = 0; i < standbyNodeNames.size(); i++) {
            log.info("Standby Site Name {}, IpAddress {}", standbyNodeNames.get(i), standbyIpAddresses.get(i));
            NodeDescriptor nodeInfo = new NodeDescriptor(standbyIpAddresses.get(i),
                    standbyLogReplicationPort, ClusterRole.STANDBY, STANDBY_SITE_NAME, UUID.fromString(standbyNodeIds.get(i)));
            standbySites.get(STANDBY_SITE_NAME).getNodesDescriptors().add(nodeInfo);
        }

        log.info("Primary Site Info {}; Backup Site Info {}", primarySite, standbySites);
        return new TopologyDescriptor(0, primarySite, standbySites);
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
    public TopologyConfigurationMsg queryTopologyConfig() {
        if (topologyConfig == null) {
            topologyConfig = constructTopologyConfigMsg();
        }

        log.debug("new cluster config msg " + topologyConfig);
        return topologyConfig;
    }

    /**
     * Change one of the standby as the primary and primary become the standby
     **/
    public static TopologyDescriptor changePrimary(TopologyConfigurationMsg topologyConfig) {
        TopologyDescriptor siteConfig = new TopologyDescriptor(topologyConfig);
        ClusterDescriptor oldPrimary = new ClusterDescriptor(siteConfig.getActiveCluster(),
                ClusterRole.STANDBY);
        Map<String, ClusterDescriptor> standbys = new HashMap<>();
        ClusterDescriptor newPrimary = null;
        ClusterDescriptor standby;

        standbys.put(oldPrimary.getClusterId(), oldPrimary);
        for (String endpoint : siteConfig.getStandbyClusters().keySet()) {
            ClusterDescriptor info = siteConfig.getStandbyClusters().get(endpoint);
            if (newPrimary == null) {
                newPrimary = new ClusterDescriptor(info, ClusterRole.ACTIVE);
            } else {
                standby = new ClusterDescriptor(info, ClusterRole.STANDBY);
                standbys.put(standby.getClusterId(), standby);
            }
        }

        TopologyDescriptor newSiteConf = new TopologyDescriptor(1, newPrimary, standbys);
        return newSiteConf;
    }

    /**
     * Testing purpose to generate cluster role change.
     */
    public static class SiteManagerCallback implements Runnable {
        public boolean siteFlip = false;
        DefaultClusterManager siteManager;

        SiteManagerCallback(DefaultClusterManager siteManagerAdapter) {
            this.siteManager = siteManagerAdapter;
        }

        @Override
        public void run() {
            while (!siteManager.ifShutdown) {
                try {
                    sleep(changeInterval);
                    if (siteFlip) {
                        TopologyDescriptor newConfig = changePrimary(siteManager.getTopologyConfig());
                        siteManager.updateTopologyConfig(newConfig.convertToMessage());
                        log.warn("change the cluster config");
                        siteFlip = false;
                    }
                } catch (Exception e) {
                    log.error("caught an exception " + e);
                }
            }
        }
    }

}
