package org.corfudb.logreplication.infrastructure;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

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

    SiteManagerCallback siteManagerCallback;
    Thread thread = new Thread(siteManagerCallback);

    DefaultSiteManager() {
    }

    public void start() {
        siteManagerCallback = new SiteManagerCallback(this);
        thread = new Thread(siteManagerCallback);
        System.out.print("\nstart the listener");
    }

    public CrossSiteConfiguration readConfig() throws IOException {
        CrossSiteConfiguration.SiteInfo primarySite;
        Map<String, CrossSiteConfiguration.SiteInfo> standbySites = new HashMap<>();
        try {
            File configFile = new File(config_file);
            FileReader reader = new FileReader(configFile);

            Properties props = new Properties();
            props.load(reader);

            Set<String> names = props.stringPropertyNames();

            // Setup primary site information
            primarySite = new CrossSiteConfiguration.SiteInfo(props.getProperty(PRIMARY_SITE_NAME, DEFAULT_PRIMARY_SITE_NAME), CrossSiteConfiguration.RoleType.PrimarySite);
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
                CrossSiteConfiguration.NodeInfo nodeInfo = new CrossSiteConfiguration.NodeInfo(ipAddress, portNum, CrossSiteConfiguration.RoleType.PrimarySite, corfuPortNum);
                primarySite.nodesInfo.add(nodeInfo);
            }

            // Setup backup site information
            standbySites = new HashMap<>();
            standbySites.put(STANDBY_SITE_NAME, new CrossSiteConfiguration.SiteInfo(props.getProperty(STANDBY_SITE_NAME, DEFAULT_STANDBY_SITE_NAME), CrossSiteConfiguration.RoleType.StandbySite));
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
                CrossSiteConfiguration.NodeInfo nodeInfo = new CrossSiteConfiguration.NodeInfo(ipAddress, portNum, CrossSiteConfiguration.RoleType.StandbySite, corfuPortNum);
                standbySites.get(STANDBY_SITE_NAME).nodesInfo.add(nodeInfo);
            }

            reader.close();
            log.info("Primary Site Info {}; Backup Site Info {}", primarySite, standbySites);
            return new CrossSiteConfiguration(primarySite, standbySites);
        } catch (Exception e) {
            log.warn("Caught an exception while reading the config file: {}", e);
            throw e;
        }
    }

    public CrossSiteConfiguration query() throws IOException {
        return readConfig();
    }

    static class SiteManagerCallback implements Runnable {
        CorfuReplicationSiteManagerAdapter siteManager;

        SiteManagerCallback(CorfuReplicationSiteManagerAdapter siteManagerAdapter) {
            this.siteManager = siteManagerAdapter;
        }

        @SneakyThrows
        @Override
        public void run() {
            while (true) {
                sleep(100000);
                siteManager.update(siteManager.query());
            }
        }
    }
}
