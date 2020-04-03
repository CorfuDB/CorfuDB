package org.corfudb.logreplication.infrastructure;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.runtime.LogReplicationRuntime;
import org.corfudb.logreplication.runtime.LogReplicationRuntimeParameters;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

@Slf4j
public class CrossSiteConfiguration {
    public static final String config_file = "/config/corfu/corfu_replication_config.properties";

    private static final String DEFAULT_CORFU_PORT_NUM = "9000";
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

            Set<String> names = props.stringPropertyNames();

            // Setup primary site information
            primarySite = new SiteInfo(props.getProperty(PRIMARY_SITE_NAME, DEFAULT_PRIMARY_SITE_NAME));
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
                NodeInfo nodeInfo = new NodeInfo(ipAddress, portNum, RoleType.PrimarySite, corfuPortNum);
                primarySite.nodesInfo.add(nodeInfo);
            }

            // Setup backup site information
            standbySites = new HashMap<>();
            standbySites.put(STANDBY_SITE_NAME, new SiteInfo(props.getProperty(STANDBY_SITE_NAME, DEFAULT_STANDBY_SITE_NAME)));
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
                NodeInfo nodeInfo = new NodeInfo(ipAddress, portNum, RoleType.StandbySite, corfuPortNum);
                standbySites.get(STANDBY_SITE_NAME).nodesInfo.add(nodeInfo);
            }

            reader.close();
            log.info("Primary Site Info {}; Backup Site Info {}", primarySite, standbySites);
        } catch (Exception e) {
            log.warn("Caught an exception while reading the config file: {}", e);
        }
    }

    public NodeInfo getNodeInfo(String endpoint) {
        List<SiteInfo> sites = new ArrayList<>(standbySites.values());

        sites.add(primarySite);
        NodeInfo nodeInfo = getNodeInfo(sites, endpoint);

        if (nodeInfo == null) {
            log.warn("No Site has node with IP  {} ", endpoint);
        }

        return nodeInfo;
    }

    private NodeInfo getNodeInfo(List<SiteInfo> sitesInfo, String endpoint) {
        for(SiteInfo site : sitesInfo) {
            for (NodeInfo nodeInfo : site.getNodesInfo()) {
                if (nodeInfo.getEndpoint().equals(endpoint)) {
                    return nodeInfo;
                }
            }
        }

        log.warn("There is no nodeInfo for ipAddress {} ", endpoint);
        return null;
    }

    static class SiteInfo {

        @Getter
        String siteId;

        @Getter
        @Setter
        NodeInfo leader;

        @Getter
        List<NodeInfo> nodesInfo;

        public SiteInfo(String siteId) {
            this.siteId = siteId;
            nodesInfo = new ArrayList<>();
        }

        public void setupLogReplicationRemoteRuntime(NodeInfo localNode) {
            for (NodeInfo nodeInfo : nodesInfo) {
                LogReplicationRuntimeParameters parameters = LogReplicationRuntimeParameters.builder()
                        .localCorfuEndpoint(localNode.getCorfuEndpoint())
                        .remoteLogReplicationServerEndpoint(nodeInfo.getEndpoint()).build();
                LogReplicationRuntime replicationRuntime = new LogReplicationRuntime(parameters);
                replicationRuntime.connect();
                nodeInfo.runtime = replicationRuntime;
            }
        }

        /**
         * Retrieve Remote Leader Endpoint
         **
         * @return remote leader endpoint.
         */
        public NodeInfo getRemoteLeader() throws Exception {
            NodeInfo leaderNode = null;
            try {
                long epoch = -1;
                LogReplicationQueryLeaderShipResponse resp;
                for (NodeInfo nodeInfo : nodesInfo) {
                    resp = nodeInfo.runtime.queryLeadership();
                    if (resp.getEpoch() > epoch && resp.isLeader()) {
                        leaderNode = nodeInfo;
                    }
                }
            } catch (Exception e) {
                log.error("Function getRemoteLeader caught an exception", e);
                throw e;
            }

            if (leaderNode != null) {
                leaderNode.setLeader(true);
            }
            this.leader = leaderNode;
            return leaderNode;
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
        String corfuPortNum;
        String portNum;
        boolean leader;
        LogReplicationRuntime runtime;

        NodeInfo(String ipAddress, String portNum, RoleType type, String corfuPortNum) {
            this.ipAddress = ipAddress;
            this.portNum = portNum;
            this.roleType = type;
            this.leader = false;
            this.corfuPortNum = corfuPortNum;
        }

        public String getEndpoint() {
            return ipAddress + ":" + portNum;
        }

        public String getCorfuEndpoint() {
            return ipAddress + ":" + corfuPortNum;
        }
        @Override
        public String toString() {
            return String.format("Role Type: %s, %s, %s", roleType, getEndpoint(), leader);
        }
    }

    enum RoleType {
        PrimarySite,
        StandbySite
    }
}