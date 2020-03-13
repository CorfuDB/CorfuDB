package org.corfudb.logreplication.infrastructure;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.runtime.LogReplicationRuntime;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Properties;

@Slf4j
public class SiteToSiteConfiguration {
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
    SiteInfo backupSite;

    public SiteToSiteConfiguration() {
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
                primarySite.nodeInfos.add(nodeInfo);
            }

            // Setup backup site information
            backupSite = new SiteInfo(props.getProperty(STANDBY_SITE_NAME, DEFAULT_STANDBY_SITE_NAME));
            portNum = props.getProperty(LOG_REPLICATION_SERVICE_STANDBY_PORT_NUM, DEFAULT_STANDBY_PORT_NUM);
            for (int i = 0; i < 3; i++) {
                String ipAddress = props.getProperty(STANDBY_SITE_NODE +"i", DEFAULT_STANDBY_IP);
                NodeInfo nodeInfo = new NodeInfo(ipAddress, portNum, RoleType.BackupSite);
                backupSite.nodeInfos.add(nodeInfo);
            }

            reader.close();
            log.info("Primary Site Info {}; Backup Site Info {}", primarySite, backupSite);
        } catch (Exception e) {
            log.warn("Caught an exception while reading the config file: {}", e.getCause());
        }
    }

    NodeInfo getNodeInfo(String ipaddress) {
        NodeInfo nodeInfo = getNodeInfo(primarySite, ipaddress);

        if (nodeInfo == null) {
            nodeInfo = getNodeInfo(backupSite, ipaddress);
        }

        if (nodeInfo == null) {
            log.warn("There is no nodeInfo for ipaddress {} ", ipaddress);
        }

        return nodeInfo;
    }

    NodeInfo getNodeInfo(SiteInfo siteInfo, String ipaddress) {
        for (NodeInfo nodeInfo: siteInfo.getNodeInfos()) {
            if (nodeInfo.getIpAddress() == ipaddress) {
                return nodeInfo;
            }
        }

        log.warn("There is no nodeInfo for ipaddress {} ", ipaddress);
        return null;
    }

    public String getLocalCorfuEndpoint() {
        // TODO (add logic here) for now hard-coding
        if (primarySite.nodeInfos.size() > 0) {
            return primarySite.nodeInfos.get(0).getIpAddress() + ":" + DEFAULT_CORFU_PORT_NUM;
        }

        return DEFAULT_PRIMARY_IP + ":" + DEFAULT_CORFU_PORT_NUM;
    }

    public String getRemoteLogReplicationServer() {
        return backupSite.getRemoteLeaderEndpoint();
    }

    public boolean isLocalPrimary() {
        // TODO: add logic (temp)
        return true;
    }

    static class SiteInfo {
        String corfuClusterName;
        @Getter
        ArrayList<NodeInfo> nodeInfos;

        SiteInfo(String name) {
            corfuClusterName = name;
            nodeInfos = new ArrayList<>();
        }

        public String getRemoteLeaderEndpoint() {
            // TODO (Add logic), for now, we'll assume the first node info is the leader endpoint in the remote site
            // (lead LRS)
            if (nodeInfos.size() > 0) {
                log.info("Remote Leader Endpoint: " + nodeInfos.get(0).getEndpoint());
                return nodeInfos.get(0).getEndpoint();
            }

            return DEFAULT_STANDBY_IP + ":" + DEFAULT_STANDBY_PORT_NUM;
        }

        @Override
        public String toString() {
            return String.format("Cluster[%s] --- Nodes[%s]:  %s", corfuClusterName, nodeInfos.size(), nodeInfos);
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