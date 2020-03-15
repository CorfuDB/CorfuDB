package org.corfudb.logreplication.infrastructure;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.runtime.LogReplicationRuntime;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class CrossSiteConfiguration {
    private static final String config_file = "/Users/amartinezman/annym/workspace/CorfuDB/log-replication/resources/corfu_replication_config.properties";

    private static final String DEFAULT_CORFU_PORT_NUM = "9000";
    private static final String DEFAULT_PRIMARY_PORT_NUM = "9010";
    private static final String DEFAULT_STANDBY_PORT_NUM = "9020";
    private static final String DEFAULT_PRIMARY_IP = "localhost";
    private static final String DEFAULT_STANDBY_IP = "localhost";
    private static final String DEFAULT_PRIMARY_SITE_NAME = "primary_site";
    private static final String DEFAULT_STANDBY_SITE_NAME = "standby_site";
    private static final int NUM_NODES_PER_CLUSTER = 3;

    private static final String PRIMARY_SITE_NAME = "primary_site";
    private static final String STANDBY_SITE_NAME = "standby_site";
    private static final String LOG_REPLICATION_SERVICE_PRIMARY_PORT_NUM = "LOG_REPLICATION_SERVICE_SENDER_PORT_NUM";
    private static final String LOG_REPLICATION_SERVICE_STANDBY_PORT_NUM = "LOG_REPLICATION_SERVICE_RECEIVER_PORT_NUM ";

    private static final String PRIMARY_SITE_NODE = "primary_site_node";
    private static final String STANDBY_SITE_NODE = "standby_site_node";

    @Getter
    SiteInfo primarySite;

    @Getter
    Map<String, SiteInfo> standbySites;

    public CrossSiteConfiguration() {
        readConfig();
    }

    private void readConfig() {
        try {
            File configFile = new File(config_file);
            FileReader reader = new FileReader(configFile);

            Properties props = new Properties();
            props.load(reader);

            /**
             * PrimarySite string
             * portnumber 20
             * primarynode1 ip
             * primarynode2 ip
             * primarynode3 ip
             *
             * BackupSite string
             * backupnode1 ip
             * backupnode2 ip
             * backupnode3 ip
             */

            // Setup primary site information
            primarySite = new SiteInfo(props.getProperty(PRIMARY_SITE_NAME, DEFAULT_PRIMARY_SITE_NAME));
            String portNum = props.getProperty(LOG_REPLICATION_SERVICE_PRIMARY_PORT_NUM, DEFAULT_PRIMARY_PORT_NUM);
            for (int i = 0; i < NUM_NODES_PER_CLUSTER; i++) {
                String ipAddress = props.getProperty(PRIMARY_SITE_NODE +"i", DEFAULT_PRIMARY_IP);
                NodeInfo nodeInfo = new NodeInfo(ipAddress, portNum, RoleType.PrimarySite);
                primarySite.nodesInfo.add(nodeInfo);
            }

            // Setup backup site information
            standbySites = new HashMap<>();
            standbySites.put(STANDBY_SITE_NAME, new SiteInfo(props.getProperty(STANDBY_SITE_NAME, DEFAULT_STANDBY_SITE_NAME)));
            portNum = props.getProperty(LOG_REPLICATION_SERVICE_STANDBY_PORT_NUM, DEFAULT_STANDBY_PORT_NUM);
            for (int i = 0; i < 3; i++) {
                String ipAddress = props.getProperty(STANDBY_SITE_NODE +"i", DEFAULT_STANDBY_IP);
                NodeInfo nodeInfo = new NodeInfo(ipAddress, portNum, RoleType.BackupSite);
                standbySites.get(STANDBY_SITE_NAME).nodesInfo.add(nodeInfo);
            }

            reader.close();
            log.info("Primary Site Info {}; Backup Site Info {}", primarySite, standbySites);
        } catch (Exception e) {
            log.warn("Caught an exception while reading the config file: {}", e.getCause());
        }
    }

    public NodeInfo getNodeInfo(String ipAddress) {
        List<SiteInfo> sites = new ArrayList<>(standbySites.values());
        sites.add(primarySite);
        NodeInfo nodeInfo = getNodeInfo(sites, ipAddress);

        if (nodeInfo == null) {
            log.warn("No Site has node with IP  {} ", ipAddress);
        }

        return nodeInfo;
    }

    private NodeInfo getNodeInfo(List<SiteInfo> sitesInfo, String ipAddress) {
        for(SiteInfo site : sitesInfo) {
            for (NodeInfo nodeInfo : site.getNodesInfo()) {
                if (nodeInfo.getIpAddress().equals(ipAddress)) {
                    return nodeInfo;
                }
            }
        }

        log.warn("There is no nodeInfo for ipAddress {} ", ipAddress);
        return null;
    }

    public String getLocalCorfuEndpoint() {
        // TODO (add logic here) for now hard-coding
        if (primarySite.nodesInfo.size() > 0) {
            return primarySite.nodesInfo.get(0).getIpAddress() + ":" + DEFAULT_CORFU_PORT_NUM;
        }

        return DEFAULT_PRIMARY_IP + ":" + DEFAULT_CORFU_PORT_NUM;
    }

    public String getRemoteLogReplicationServer(String site) {
        return standbySites.get(site).getRemoteLeaderEndpoint();
    }

    public boolean isLocalSource() {
        // TODO: add logic (temp)
        return true;
    }

    /**
     * Return all sites remote leader endpoints.
     *
     * For Multi-Site Replication, we'll have one remote leader per site.
     */
    public Map<String, String> getRemoteLeaderEndpoints() {
        Map<String, String> remoteSiteToLeadEndpoint = new HashMap<>();
        for (Map.Entry<String, SiteInfo> entry : standbySites.entrySet()) {
            remoteSiteToLeadEndpoint.put(entry.getKey(), entry.getValue().getRemoteLeaderEndpoint());
        }

        return remoteSiteToLeadEndpoint;
    }

    static class SiteInfo {

        @Getter
        String siteId;

        @Getter
        List<NodeInfo> nodesInfo;

        public SiteInfo(String siteId) {
            this.siteId = siteId;
            nodesInfo = new ArrayList<>();
        }

        /**
         * Retrieve Remote Leader Endpoint
         **
         * @return remote leader endpoint.
         */
        public String getRemoteLeaderEndpoint() {
            // TODO (Add logic), for now, we'll assume the first node info is the leader endpoint in the remote site
            // (lead LRS)
            if (nodesInfo.size() > 0) {
                log.info("Remote Leader Endpoint: " + nodesInfo.get(0).getEndpoint());
                return nodesInfo.get(0).getEndpoint();
            }

            return DEFAULT_STANDBY_IP + ":" + DEFAULT_STANDBY_PORT_NUM;
        }

        @Override
        public String toString() {
            return String.format("Cluster[%s] --- Nodes[%s]:  %s", siteId, nodesInfo.size(), nodesInfo);
        }
    }

    @Data
    public class NodeInfo {
        RoleType roleType;
        String ipAddress;
        String portNum;
        boolean lockHolder;
        LogReplicationRuntime runtime;

        NodeInfo(String ipAddress, String portNum, RoleType type) {
            this.ipAddress = ipAddress;
            this.portNum = portNum;
            this.roleType = type;
            this.lockHolder = false;
        }

        public String getEndpoint() {
            return ipAddress + ":" + portNum;
        }

        @Override
        public String toString() {
            return String.format("Role Type: %s, %s:%s, %s", roleType, ipAddress, portNum, lockHolder);
        }
    }

    enum RoleType {
        PrimarySite,
        BackupSite
    }
}