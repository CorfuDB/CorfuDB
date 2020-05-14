package org.corfudb.logreplication.infrastructure;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationTransportType;
import org.corfudb.logreplication.runtime.LogReplicationRuntime;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Slf4j
public class CrossSiteConfiguration {

    public static final String config_file = "/config/corfu/corfu_replication_config.properties";

    private static final String DEFAULT_PRIMARY_SITE_NAME = "primary_site";
    private static final String DEFAULT_STANDBY_SITE_NAME = "standby_site";
    private static final int NUM_NODES_PER_CLUSTER = 1;

    private static final String PRIMARY_SITE_NAME = "primary_site";
    private static final String STANDBY_SITE_NAME = "standby_site";
    private static final String PRIMARY_SITE_CORFU_PORTNUM = "primary_site_corfu_portnumber";
    private static final String STANDBY_SITE_CORFU_PORTNUM = "standby_site_corfu_portnumber";
    private static final String LOG_REPLICATION_SERVICE_PRIMARY_PORT_NUM = "primary_site_portnumber";
    private static final String LOG_REPLICATION_SERVICE_STANDBY_PORT_NUM = "standby_site_portnumber";


    private static final String PRIMARY_SITE_NODE = "primary_site_node";
    private static final String STANDBY_SITE_NODE = "standby_site_node";

    @Getter
    Site primarySite;

    @Getter
    Map<String, Site> standbySites;

    public CrossSiteConfiguration() {
        readConfig();
    }

    private void readConfig() {
        try {
            // TODO: [TEMP] Reading Site Info from a config file ---until this is pulled from an external system
            // providing Site Info (Site Manager)--- This will be removed
            File configFile = new File(config_file);
            FileReader reader = new FileReader(configFile);

            Properties props = new Properties();
            props.load(reader);

            Set<String> names = props.stringPropertyNames();

            // Setup primary site information
            primarySite = new Site(props.getProperty(PRIMARY_SITE_NAME, DEFAULT_PRIMARY_SITE_NAME));
            String corfuPortNum = props.getProperty(PRIMARY_SITE_CORFU_PORTNUM);
            String portNum = props.getProperty(LOG_REPLICATION_SERVICE_PRIMARY_PORT_NUM);

            for (int i = 0; i < NUM_NODES_PER_CLUSTER; i++) {
                String nodeName = PRIMARY_SITE_NODE + i;
                if (!names.contains(nodeName)) {
                    continue;
                }
                String ipAddress = props.getProperty(nodeName);
                log.info("Primary site[{}] Node {} on {}:{}", primarySite.getSiteId(), nodeName, ipAddress, portNum);
                NodeInfo nodeInfo = new NodeInfo(ipAddress, portNum, RoleType.PrimarySite, corfuPortNum);
                primarySite.nodesInfo.add(nodeInfo);
            }

            // Setup backup site information
            standbySites = new HashMap<>();
            standbySites.put(STANDBY_SITE_NAME, new Site(props.getProperty(STANDBY_SITE_NAME, DEFAULT_STANDBY_SITE_NAME)));
            corfuPortNum = props.getProperty(STANDBY_SITE_CORFU_PORTNUM);
            portNum = props.getProperty(LOG_REPLICATION_SERVICE_STANDBY_PORT_NUM);

            for (int i = 0; i < NUM_NODES_PER_CLUSTER; i++) {
                String nodeName = STANDBY_SITE_NODE + i;
                if (!names.contains(nodeName)) {
                    continue;
                }
                String ipAddress = props.getProperty(STANDBY_SITE_NODE + i);
                log.trace("Standby site[{}] Node {} on {}:{}", standbySites.get(STANDBY_SITE_NAME).getSiteId(), nodeName, ipAddress, portNum);
                NodeInfo nodeInfo = new NodeInfo(ipAddress, portNum, RoleType.StandbySite, corfuPortNum);
                standbySites.get(STANDBY_SITE_NAME).nodesInfo.add(nodeInfo);
            }

            reader.close();
            log.info("Primary Site: {}; Standby Site(s): {}", primarySite, standbySites);
        } catch (Exception e) {
            log.warn("Caught an exception while reading the config file: {}", e);
        }
    }

    public NodeInfo getNodeInfo(String endpoint) {
        List<Site> sites = new ArrayList<>(standbySites.values());

        sites.add(primarySite);
        NodeInfo nodeInfo = getNodeInfo(sites, endpoint);

        if (nodeInfo == null) {
            log.warn("No Site has node with IP {} ", endpoint);
        }

        return nodeInfo;
    }

    private NodeInfo getNodeInfo(List<Site> sitesInfo, String endpoint) {
        for(Site site : sitesInfo) {
            for (NodeInfo nodeInfo : site.getNodesInfo()) {
                if (nodeInfo.getEndpoint().equals(endpoint)) {
                    return nodeInfo;
                }
            }
        }

        log.warn("There is no nodeInfo for ipAddress {} ", endpoint);
        return null;
    }

    public class Site {

        @Getter
        String siteId;

        @Getter
        @Setter
        NodeInfo leader;

        @Getter
        List<NodeInfo> nodesInfo;

        public Site(String siteId) {
            this.siteId = siteId;
            nodesInfo = new ArrayList<>();
        }

        public void connect(NodeInfo localNode, LogReplicationTransportType transport) {
            // TODO (Xiaoqin Ma): shouldn't it connect only to the lead node on the remote site?
            for (NodeInfo nodeInfo : nodesInfo) {
                LogReplicationRuntimeParameters parameters = LogReplicationRuntimeParameters.builder()
                        .localCorfuEndpoint(localNode.getCorfuEndpoint())
                        .remoteLogReplicationServerEndpoint(nodeInfo.getEndpoint())
                        .transport(transport)
                        .build();
                LogReplicationRuntime replicationRuntime = new LogReplicationRuntime(parameters);
                replicationRuntime.connect();
                nodeInfo.setRuntime(replicationRuntime);
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