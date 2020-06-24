package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DefaultSiteConfig;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.SiteStatus;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.SiteConfigurationMsg;

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

import static java.lang.Thread.sleep;

@Slf4j
public class DefaultSiteManager extends CorfuReplicationSiteManagerImpl {
    public static long epoch = 0;
    public static final int changeInterval = 5000;
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


    public static CrossSiteConfiguration readConfig() throws IOException {
        CrossSiteConfiguration.SiteInfo primarySite;
        Map<String, CrossSiteConfiguration.SiteInfo> standbySites = new HashMap<>();
        List<String> primaryNodeNames = new ArrayList<>();
        List<String> standbyNodeNames = new ArrayList<>();
        List<String> primaryIpAddresses = new ArrayList<>();
        List<String> standbyIpAddresses = new ArrayList<>();
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
            primaryCorfuPort = props.getProperty(PRIMARY_SITE_CORFU_PORTNUM);
            primaryLogReplicationPort = props.getProperty(LOG_REPLICATION_SERVICE_PRIMARY_PORT_NUM);
            for (int i = 0; i < NUM_NODES_PER_CLUSTER; i++) {
                String nodeName = PRIMARY_SITE_NODE + i;
                if (!names.contains(nodeName)) {
                    continue;
                }
                primaryNodeNames.add(nodeName);
                primaryIpAddresses.add(props.getProperty(nodeName));
            }

            standbySiteName = props.getProperty(STANDBY_SITE_NAME, DEFAULT_STANDBY_SITE_NAME);
            standbyCorfuPort = props.getProperty(STANDBY_SITE_CORFU_PORTNUM);
            standbyLogReplicationPort = props.getProperty(LOG_REPLICATION_SERVICE_STANDBY_PORT_NUM);
            for (int i = 0; i < NUM_NODES_PER_CLUSTER; i++) {
                String nodeName = STANDBY_SITE_NODE + i;
                if (!names.contains(nodeName)) {
                    continue;
                }
                standbyNodeNames.add(nodeName);
                standbyIpAddresses.add(props.getProperty(nodeName));
            }
            reader.close();
        } catch (FileNotFoundException e) {
            log.warn("Site Config File {} does not exist.  Using default configs", config_file);
            primarySiteName = DefaultSiteConfig.getPrimarySiteName();
            primaryCorfuPort = DefaultSiteConfig.getPrimaryCorfuPort();
            primaryLogReplicationPort = DefaultSiteConfig.getPrimaryLogReplicationPort();
            primaryNodeNames.addAll(DefaultSiteConfig.getPrimaryNodeNames());
            primaryIpAddresses.addAll(DefaultSiteConfig.getPrimaryIpAddresses());

            standbySiteName = DefaultSiteConfig.getStandbySiteName();
            standbyCorfuPort = DefaultSiteConfig.getStandbyCorfuPort();
            standbyLogReplicationPort = DefaultSiteConfig.getStandbyLogReplicationPort();
            standbyNodeNames.addAll(DefaultSiteConfig.getStandbyNodeNames());
            standbyIpAddresses.addAll(DefaultSiteConfig.getStandbyIpAddresses());
        }
        primarySite = new CrossSiteConfiguration.SiteInfo(primarySiteName, SiteStatus.ACTIVE);

        for (int i = 0; i < primaryNodeNames.size(); i++) {
            log.info("Primary Site Name {}, IpAddress {}", primaryNodeNames.get(i), primaryIpAddresses.get(i));
            LogReplicationNodeInfo nodeInfo = new LogReplicationNodeInfo(primaryIpAddresses.get(i),
                    primaryLogReplicationPort, SiteStatus.ACTIVE, primaryCorfuPort, PRIMARY_SITE_NAME);
            primarySite.nodesInfo.add(nodeInfo);
        }

        // Setup backup site information
        standbySites = new HashMap<>();
        standbySites.put(STANDBY_SITE_NAME, new CrossSiteConfiguration.SiteInfo(standbySiteName, SiteStatus.STANDBY));

        for (int i = 0; i < standbyNodeNames.size(); i++) {
            log.trace("Standby Site Name {}, IpAddress {}", standbyNodeNames.get(i), standbyIpAddresses.get(i));
            LogReplicationNodeInfo nodeInfo = new LogReplicationNodeInfo(standbyIpAddresses.get(i),
                    standbyLogReplicationPort, SiteStatus.STANDBY, standbyCorfuPort, STANDBY_SITE_NAME);
            standbySites.get(STANDBY_SITE_NAME).nodesInfo.add(nodeInfo);
        }

        log.info("Primary Site Info {}; Backup Site Info {}", primarySite, standbySites);
        return new CrossSiteConfiguration(0, primarySite, standbySites);
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
    public static CrossSiteConfiguration changePrimary(SiteConfigurationMsg siteConfigMsg) {
        CrossSiteConfiguration siteConfig = new CrossSiteConfiguration(siteConfigMsg);
        CrossSiteConfiguration.SiteInfo oldPrimary = new CrossSiteConfiguration.SiteInfo(siteConfig.getPrimarySite(),
                SiteStatus.STANDBY);
        Map<String, CrossSiteConfiguration.SiteInfo> standbys = new HashMap<>();
        CrossSiteConfiguration.SiteInfo newPrimary = null;
        CrossSiteConfiguration.SiteInfo standby;

        standbys.put(oldPrimary.getSiteId(), oldPrimary);
        for (String endpoint : siteConfig.getStandbySites().keySet()) {
            CrossSiteConfiguration.SiteInfo info = siteConfig.getStandbySites().get(endpoint);
            if (newPrimary == null) {
                newPrimary = new CrossSiteConfiguration.SiteInfo(info, SiteStatus.ACTIVE);
            } else {
                standby = new CrossSiteConfiguration.SiteInfo(info, SiteStatus.STANDBY);
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
            while (!siteManager.ifShutdown) {
                try {
                    sleep(changeInterval);
                    if (siteFlip) {
                        CrossSiteConfiguration newConfig = changePrimary(siteManager.getSiteConfigMsg());
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
