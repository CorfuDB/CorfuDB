package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.cluster.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.cluster.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.cluster.ClusterDescriptor;
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
public class DefaultClusterManager extends CorfuReplicationSiteManagerAdapter {
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

    private static final String PRIMARY_SITE_NODE = "primary_site_node";
    private static final String STANDBY_SITE_NODE = "standby_site_node";
    private boolean ifShutdown = false;

    @Getter
    public SiteManagerCallback siteManagerCallback;

    Thread thread = new Thread(siteManagerCallback);

    /**
     * TestCase 0:
     * 1. Do a normal boot up, server A as active and server B as standby.
     * 2. Do a normal site flip over:
     *    Wait till the replication complete, A becomes standby first, then B becomes active.
     *
     * TestCase 1:
     * Both servers boot up as Active.
     * Observe:
     * -- Data are sent to each other, but are dropped at both sites.
     * -- As there is no ACK sent back, the active should stop sending message.
     * -- Until being notified with a new config, both servers start to work again.
     *
     * TestCase 2:
     * Both servers boot up as Standby.
     * Observe:
     * -- As both are standbys, there will be no data sent around, until being notified with a new config.
     *
     * TestCase 3:
     * 1. Do a normal boot up, server A as active and server B as standby with configID = 0;
     * 2. Do an abnomal flip, both A and B becomes active with configID 1.
     *    Notify B with configID = 1 and A as standby and B as active.
     *    Notify A with configID = 1 and A as active and B as standby.
     *
     * TestCase 4:
     * 1. Do a normal bootup, server A as active and server B as standby with configID = 0;
     * 2. Do an abnormal flip, both A and B becomes standby with configID 1.
     * -- Notify B with configID = 1 and A as active and B as standby.
     * -- Notify A with configID = 1 and A as standby and B as active.
     *
     */
    int testCase;

    String localEndpoint;

    public DefaultClusterManager() {
        this.testCase = 0;
    }

    public DefaultClusterManager(int testCase) {
        this.testCase = testCase;
    }

    public void setLocalEndpoint(String endpoint) {
        this.localEndpoint = endpoint;
        siteManagerCallback.setLocalEndpoint(endpoint);
    }

    public void start() {
        siteManagerCallback = new SiteManagerCallback(this, testCase);
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
        String primarySiteName;
        String primaryCorfuPort;
        String primaryLogReplicationPort;

        String standbySiteName;
        String standbyCorfuPort;
        String standbyLogReplicationPort;

        File configFile = new File(config_file);
        try {
            FileReader reader = new FileReader(configFile);
            Properties props = new Properties();
            props.load(reader);

            Set<String> names = props.stringPropertyNames();

            primarySiteName = props.getProperty(PRIMARY_SITE_NAME, DEFAULT_PRIMARY_SITE_NAME);
            primaryCorfuPort = props.getProperty(PRIMARY_SITE_CORFU_PORT);
            primaryLogReplicationPort = props.getProperty(LOG_REPLICATION_SERVICE_PRIMARY_PORT_NUM);
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
            primarySiteName = DefaultClusterConfig.getActiveClusterId();
            primaryCorfuPort = DefaultClusterConfig.getActiveCorfuPort();
            primaryLogReplicationPort = DefaultClusterConfig.getActiveLogReplicationPort();
            primaryNodeNames.addAll(DefaultClusterConfig.getActiveNodeNames());
            primaryIpAddresses.addAll(DefaultClusterConfig.getActiveIpAddresses());
            primaryNodeIds.addAll(DefaultClusterConfig.getActiveNodesUuid());

            standbySiteName = DefaultClusterConfig.getStandbyClusterId();
            standbyCorfuPort = DefaultClusterConfig.getStandbyCorfuPort();
            standbyLogReplicationPort = DefaultClusterConfig.getStandbyLogReplicationPort();
            standbyNodeNames.addAll(DefaultClusterConfig.getStandbyNodeNames());
            standbyIpAddresses.addAll(DefaultClusterConfig.getStandbyIpAddresses());
            standbyNodeIds.addAll(DefaultClusterConfig.getStandbyNodesUuid());
        }

        primarySite = new ClusterDescriptor(primarySiteName, ClusterRole.ACTIVE);

        for (int i = 0; i < primaryNodeNames.size(); i++) {
            log.info("Primary Site Name {}, IpAddress {}", primaryNodeNames.get(i), primaryIpAddresses.get(i));
            NodeDescriptor nodeInfo = new NodeDescriptor(primaryIpAddresses.get(i),
                    primaryLogReplicationPort, ClusterRole.ACTIVE, primaryCorfuPort, PRIMARY_SITE_NAME, UUID.fromString(primaryNodeIds.get(i)));
            primarySite.getNodesDescriptors().add(nodeInfo);
        }

        // Setup backup cluster information
        Map<String, ClusterDescriptor> standbySites = new HashMap<>();
        standbySites.put(STANDBY_SITE_NAME, new ClusterDescriptor(standbySiteName, ClusterRole.STANDBY));

        for (int i = 0; i < standbyNodeNames.size(); i++) {
            log.info("Standby Site Name {}, IpAddress {}", standbyNodeNames.get(i), standbyIpAddresses.get(i));
            NodeDescriptor nodeInfo = new NodeDescriptor(standbyIpAddresses.get(i),
                    standbyLogReplicationPort, ClusterRole.STANDBY, standbyCorfuPort, STANDBY_SITE_NAME, UUID.fromString(standbyNodeIds.get(i)));
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

            TopologyDescriptor siteConfig = new TopologyDescriptor(topologyConfig);;

            switch (testCase) {
                case 0:
                case 3:
                case 4:
                    //do nothing;
                    break;
                case 1:
                    // bootup both servers as active
                    return setSiteAsPrimary(siteConfig, localEndpoint).convertToMessage();

                case 2:
                    // bootup both servers as standby
                    return setSiteAsStandby(siteConfig, localEndpoint).convertToMessage();
                default:
                    log.warn("Use default behavior as testcase 0");
            }
        }

        log.debug("new cluster config msg " + topologyConfig);
        return topologyConfig;
    }

    /**
     * Change one of the standby as the primary and primary become the standby
     **/
    public static TopologyDescriptor changePrimary(TopologyDescriptor siteConfig) {
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

        TopologyDescriptor newSiteConf = new TopologyDescriptor(siteConfig.getTopologyConfigId() + 1, newPrimary, standbys);
        return newSiteConf;
    }


    /**
     * Set the site that contains the localEndpoint as active site.
     * @param siteConfig
     * @param localEndpoint
     * @return
     */
    public static TopologyDescriptor setSiteAsPrimary(TopologyDescriptor siteConfig, String localEndpoint) {
        NodeDescriptor nodeInfo = siteConfig.getNodeInfo(localEndpoint);

        if (nodeInfo.getRoleType() == ClusterRole.ACTIVE) {
            siteConfig.setTopologyConfigId(siteConfig.getTopologyConfigId() + 1);
        } else {
            ClusterDescriptor oldPrimary = new ClusterDescriptor(siteConfig.getActiveCluster(),
                    ClusterRole.STANDBY);
            Map<String, ClusterDescriptor> standbys = new HashMap<>();
            ClusterDescriptor newPrimary = null;
            ClusterDescriptor standby;

            standbys.put(oldPrimary.getClusterId(), oldPrimary);
            for (String endpoint : siteConfig.getStandbyClusters().keySet()) {
                ClusterDescriptor info = siteConfig.getStandbyClusters().get(endpoint);
                if (localEndpoint == endpoint) {
                    newPrimary = new ClusterDescriptor(info, ClusterRole.ACTIVE);
                } else {
                    standby = new ClusterDescriptor(info, ClusterRole.STANDBY);
                    standbys.put(standby.getClusterId(), standby);
                }
            }

            TopologyDescriptor newSiteConf = new TopologyDescriptor(1, newPrimary, standbys);
            return newSiteConf;
        }
        return siteConfig;
    }

    //TODO:

    /**
     * Set the site contains the localEndpoint as the standby site if it is the active.
     * @param curTopology
     * @param localEndpoint
     * @return
     */
    public static TopologyDescriptor setSiteAsStandby(TopologyDescriptor curTopology, String localEndpoint) {
        return curTopology;
    }


    /**
     * Testing purpose to generate cluster role change.
     */
    public static class SiteManagerCallback implements Runnable {
        public boolean siteFlip = false;
        DefaultClusterManager siteManager;
        int testCase;

        @Setter
        String localEndpoint;

        SiteManagerCallback(DefaultClusterManager siteManagerAdapter, int testCase) {
            this.siteManager = siteManagerAdapter;
            this.testCase = testCase;
        }

        TopologyDescriptor generateNewConfig(TopologyConfigurationMsg curTopology) {
            TopologyDescriptor newTopology = new TopologyDescriptor(curTopology);;
            switch (testCase) {
                case 0:
                    //Normal flip:
                    return changePrimary(newTopology);

                case 3:
                    // set the local site as active, all others as standby.
                    return setSiteAsPrimary(newTopology, localEndpoint);
                case 4:
                    // set the local site as standby, the other one as active.
                    return setSiteAsStandby(newTopology, localEndpoint);
                default:
                    log.warn("No siteConfig Change");
            }
            return newTopology;
        }

        @Override
        public void run() {
            while (!siteManager.ifShutdown) {
                try {
                    sleep(changeInterval);
                    if (siteFlip) {
                        TopologyDescriptor newConfig = generateNewConfig(siteManager.getTopologyConfig());
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
