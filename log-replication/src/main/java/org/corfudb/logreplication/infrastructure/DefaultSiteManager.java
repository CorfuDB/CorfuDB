package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.GlobalManagerStatus;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.SiteConfigurationMsg;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.lang.Thread.sleep;

@Slf4j
public class DefaultSiteManager extends CorfuReplicationSiteManagerAdapter {
    public static long epoch = 0;
    public static final int changeInveral = 5000;
    public static final String config_file = "/config/corfu/corfu_replication_config.properties";
    private static final String DEFAULT_PRIMARY_SITE_NAME = "primary_site";
    private static final String DEFAULT_STANDBY_SITE_NAME = "standby_site";
    private static final int NUM_NODES_PER_CLUSTER = 3;

    private static final String PRIMARY_SITE_NAME = "primary_site";
    private static final String STANDBY_SITE_NAME = "standby_site";
    private static final String PRIMARY_SITE_CORFU_PORTNUM = "primary_site_corfu_portnumber";
    private static final String STANDBY_SITE_CORFU_PORTNUM = "standby_site_corfu_portnumber";
    private static final String LOG_REPLICATION_SERVICE_PRIMARY_PORT_NUM = "primary_site_portnumber";
    private static final String LOG_REPLICATION_SERVICE_STANDBY_PORT_NUM = "standby_site_portnumber";


    private static final String PRIMARY_SITE_NODE = "primary_site_node";
    private static final String STANDBY_SITE_NODE = "standby_site_node";

    @Getter
    public SiteManagerCallback siteManagerCallback;

    Thread thread = new Thread(siteManagerCallback);

    DefaultSiteManager() {
    }

    public void start() {
        siteManagerCallback = new SiteManagerCallback(this);
        thread = new Thread(siteManagerCallback);
        thread.start();
    }

    public static CrossSiteConfiguration readConfig() throws IOException {
        CrossSiteConfiguration.SiteInfo primarySite;
        Map<String, CrossSiteConfiguration.SiteInfo> standbySites = new HashMap<>();
        try {
            File configFile = new File(config_file);
            FileReader reader = new FileReader(configFile);

            Properties props = new Properties();
            props.load(reader);

            Set<String> names = props.stringPropertyNames();

            // Setup primary site information
            primarySite = new CrossSiteConfiguration.SiteInfo(props.getProperty(PRIMARY_SITE_NAME, DEFAULT_PRIMARY_SITE_NAME), GlobalManagerStatus.ACTIVE);
            String corfuPortNum = props.getProperty(PRIMARY_SITE_CORFU_PORTNUM);
            String portNum = props.getProperty(LOG_REPLICATION_SERVICE_PRIMARY_PORT_NUM);


            for (int i = 0; i < NUM_NODES_PER_CLUSTER; i++) {
                String nodeName = PRIMARY_SITE_NODE + i;
                log.info("primary site ipaddress for node {}", nodeName);
                if (!names.contains(nodeName)) {
                    continue;
                }
                String ipAddress = props.getProperty(nodeName);
                log.info("primary site ipaddress {} for node {}", ipAddress, nodeName);
                CrossSiteConfiguration.NodeInfo nodeInfo = new CrossSiteConfiguration.NodeInfo(ipAddress, portNum, GlobalManagerStatus.ACTIVE, corfuPortNum);
                primarySite.nodesInfo.add(nodeInfo);
            }

            // Setup backup site information
            standbySites = new HashMap<>();
            standbySites.put(STANDBY_SITE_NAME, new CrossSiteConfiguration.SiteInfo(props.getProperty(STANDBY_SITE_NAME, DEFAULT_STANDBY_SITE_NAME), GlobalManagerStatus.STANDBY));
            corfuPortNum = props.getProperty(STANDBY_SITE_CORFU_PORTNUM);
            portNum = props.getProperty(LOG_REPLICATION_SERVICE_STANDBY_PORT_NUM);
            for (int i = 0; i < NUM_NODES_PER_CLUSTER; i++) {
                String nodeName = STANDBY_SITE_NODE + i;
                log.info("standby site ipaddress for node {}", nodeName);
                if (!names.contains(nodeName)) {
                    continue;
                }
                String ipAddress = props.getProperty(STANDBY_SITE_NODE + i);
                log.info("standby site ipaddress {} for node {}", ipAddress, i);
                CrossSiteConfiguration.NodeInfo nodeInfo = new CrossSiteConfiguration.NodeInfo(ipAddress, portNum, GlobalManagerStatus.STANDBY, corfuPortNum);
                standbySites.get(STANDBY_SITE_NAME).nodesInfo.add(nodeInfo);
            }

            reader.close();
            log.info("Primary Site Info {}; Backup Site Info {}", primarySite, standbySites);
            return new CrossSiteConfiguration(0, primarySite, standbySites);
        } catch (Exception e) {
            log.warn("Caught an exception while reading the config file: {}", e);
            throw e;
        }
    }


    public static SiteConfigurationMsg constructSiteConfigMsg() {
        CrossSiteConfiguration crossSiteConfiguration = null;
        SiteConfigurationMsg siteConfigurationMsg = null;

        try {
            crossSiteConfiguration = readConfig();
        } catch (Exception e) {
            log.warn("caught an exception " + e);
        }

        siteConfigurationMsg = crossSiteConfiguration.convert2msg();
        return siteConfigurationMsg;
    }

    @Override
    public SiteConfigurationMsg querySiteConfig() {
        if (siteConfigMsg == null) {
            siteConfigMsg = constructSiteConfigMsg();
        }

        log.debug("new site config msg " + siteConfigMsg);
        return siteConfigMsg;
    }

    /**
     * Change one of the standby as the primary and primary become the standby
     * @return
     */
    public static CrossSiteConfiguration changePrimary(CrossSiteConfiguration siteConfig) {
        CrossSiteConfiguration.SiteInfo oldPrimary = new CrossSiteConfiguration.SiteInfo(siteConfig.getPrimarySite(),
                GlobalManagerStatus.STANDBY);
        Map<String, CrossSiteConfiguration.SiteInfo> standbys = new HashMap<>();
        CrossSiteConfiguration.SiteInfo newPrimary = null;
        CrossSiteConfiguration.SiteInfo standby;

        standbys.put(oldPrimary.getSiteId(), oldPrimary);
        for (String endpoint : siteConfig.getStandbySites().keySet()) {
            CrossSiteConfiguration.SiteInfo info = siteConfig.getStandbySites().get(endpoint);
            if (newPrimary == null) {
                newPrimary = new CrossSiteConfiguration.SiteInfo(info, GlobalManagerStatus.ACTIVE);
            } else {
                standby = new CrossSiteConfiguration.SiteInfo(info, GlobalManagerStatus.STANDBY);
                standbys.put(standby.getSiteId(), standby);
            }
        }

        CrossSiteConfiguration newSiteConf = new CrossSiteConfiguration(1, newPrimary, standbys);
        return newSiteConf;
    }

    /**
     * Testing purpose to generate site role change.
     */
    public static class SiteManagerCallback implements Runnable {
        public boolean siteFlip = false;
        DefaultSiteManager siteManager;

        SiteManagerCallback(DefaultSiteManager siteManagerAdapter) {
            this.siteManager = siteManagerAdapter;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    sleep(changeInveral);
                    if (siteFlip) {
                        CrossSiteConfiguration newConfig = changePrimary(siteManager.getSiteConfig());
                        siteManager.updateSiteConfig(newConfig.convert2msg());
                        log.warn("change the site config");
                        siteFlip = false;
                    }
                } catch (Exception e) {
                    log.error("caught an exception " + e);
                }
            }
        }
    }
}
