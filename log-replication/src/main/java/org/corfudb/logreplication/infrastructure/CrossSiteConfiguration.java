package org.corfudb.logreplication.infrastructure;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.maven.model.Site;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.AphInfoMsg;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.GlobalManagerStatus;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.SiteConfigurationMsg;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.SiteMsg;

import org.corfudb.infrastructure.LogReplicationTransportType;
import org.corfudb.logreplication.runtime.LogReplicationRuntime;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class CrossSiteConfiguration {
    final static String DEFAULT_CORFU_PORT_NUM = "9000";

    @Setter
    @Getter
    static String CorfuPortnum = DEFAULT_CORFU_PORT_NUM;

    @Getter
    private long epoch;

    @Getter
    private SiteInfo primarySite;

    @Getter
    Map<String, SiteInfo> standbySites;

    public CrossSiteConfiguration(SiteConfigurationMsg siteConfigMsg) {
        this.epoch = siteConfigMsg.getEpoch();
        standbySites = new HashMap<>();
        for (SiteMsg siteMsg : siteConfigMsg.getSiteList()) {
            SiteInfo siteInfo = new SiteInfo(siteMsg);
            if (siteMsg.getGmStatus() == GlobalManagerStatus.ACTIVE) {
                primarySite = siteInfo;
            } else if (siteMsg.getGmStatus() == GlobalManagerStatus.STANDBY) {
                standbySites.put(siteMsg.getId(), siteInfo);
            }
        }
    }

    public CrossSiteConfiguration(long epoch, SiteInfo primarySite, Map<String, SiteInfo> standbySites) {
        this.epoch = epoch;
        this.primarySite = primarySite;
        this.standbySites = standbySites;
    }

    public SiteConfigurationMsg convert2msg() {
        ArrayList<SiteMsg> siteMsgs = new ArrayList<>();
        siteMsgs.add((primarySite.convert2msg()));

        for (SiteInfo siteInfo : standbySites.values()) {
            siteMsgs.add(siteInfo.convert2msg());
        }

        SiteConfigurationMsg configMsg = SiteConfigurationMsg.newBuilder().setEpoch(epoch).addAllSite(siteMsgs).build();

        return configMsg;
    }


//    private void readConfig() {
//        try {
//            // TODO: [TEMP] Reading Site Info from a config file ---until this is pulled from an external system
//            // providing Site Info (Site Manager)--- This will be removed
//            File configFile = new File(config_file);
//            FileReader reader = new FileReader(configFile);
//
//            Properties props = new Properties();
//            props.load(reader);
//
//            Set<String> names = props.stringPropertyNames();
//
//            // Setup primary site information
//            primarySite = new Site(props.getProperty(PRIMARY_SITE_NAME, DEFAULT_PRIMARY_SITE_NAME));
//            String corfuPortNum = props.getProperty(PRIMARY_SITE_CORFU_PORTNUM);
//            String portNum = props.getProperty(LOG_REPLICATION_SERVICE_PRIMARY_PORT_NUM);
//
//            for (int i = 0; i < NUM_NODES_PER_CLUSTER; i++) {
//                String nodeName = PRIMARY_SITE_NODE + i;
//                if (!names.contains(nodeName)) {
//                    continue;
//                }
//                String ipAddress = props.getProperty(nodeName);
//                log.info("Primary site[{}] Node {} on {}:{}", primarySite.getSiteId(), nodeName, ipAddress, portNum);
//                NodeInfo nodeInfo = new NodeInfo(ipAddress, portNum, RoleType.PrimarySite, corfuPortNum);
//                primarySite.nodesInfo.add(nodeInfo);
//            }
//
//            // Setup backup site information
//            standbySites = new HashMap<>();
//            standbySites.put(STANDBY_SITE_NAME, new Site(props.getProperty(STANDBY_SITE_NAME, DEFAULT_STANDBY_SITE_NAME)));
//            corfuPortNum = props.getProperty(STANDBY_SITE_CORFU_PORTNUM);
//            portNum = props.getProperty(LOG_REPLICATION_SERVICE_STANDBY_PORT_NUM);
//
//            for (int i = 0; i < NUM_NODES_PER_CLUSTER; i++) {
//                String nodeName = STANDBY_SITE_NODE + i;
//                if (!names.contains(nodeName)) {
//                    continue;
//                }
//                String ipAddress = props.getProperty(STANDBY_SITE_NODE + i);
//                log.trace("Standby site[{}] Node {} on {}:{}", standbySites.get(STANDBY_SITE_NAME).getSiteId(), nodeName, ipAddress, portNum);
//                NodeInfo nodeInfo = new NodeInfo(ipAddress, portNum, RoleType.StandbySite, corfuPortNum);
//                standbySites.get(STANDBY_SITE_NAME).nodesInfo.add(nodeInfo);
//            }
//
//            reader.close();
//            log.info("Primary Site: {}; Standby Site(s): {}", primarySite, standbySites);
//        } catch (Exception e) {
//            log.warn("Caught an exception while reading the config file: {}", e);
//        }
//>>>>>>> Corfu Log Replication Plugin Transport Layer

    public NodeInfo getNodeInfo(String endpoint) {
        List<SiteInfo> sites = new ArrayList<>(standbySites.values());

        sites.add(primarySite);
        NodeInfo nodeInfo = getNodeInfo(sites, endpoint);

        if (nodeInfo == null) {
            log.warn("No Site has node with IP {} ", endpoint);
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

    public static class SiteInfo {

        @Getter
        String siteId;

        @Getter
        GlobalManagerStatus roleType; //standby or active

        @Getter
        @Setter
        NodeInfo leader;

        @Getter
        List<NodeInfo> nodesInfo;

        public SiteInfo(SiteMsg siteMsg) {
            this.siteId = siteMsg.getId();
            this.roleType = siteMsg.getGmStatus();
            this.leader = null;
            this.nodesInfo = new ArrayList<>();
            for (AphInfoMsg aphInfoMsg : siteMsg.getAphList()) {
                NodeInfo newNode = new NodeInfo(aphInfoMsg.getAddress(), Integer.toString(aphInfoMsg.getPort()), siteMsg.getGmStatus(), Integer.toString(aphInfoMsg.getCorfuPort()));
                this.nodesInfo.add(newNode);
            }
        }

        public SiteInfo(SiteInfo info, GlobalManagerStatus roleType) {
            this.siteId = info.siteId;
            this.roleType = roleType;
            this.leader = info.leader;
            this.nodesInfo = new ArrayList<>();
            for ( NodeInfo nodeInfo : info.nodesInfo) {
                NodeInfo newNode = new NodeInfo(nodeInfo.getIpAddress(), nodeInfo.getPortNum(), roleType, nodeInfo.corfuPortNum);
                this.nodesInfo.add(newNode);
            }
        }

        public SiteInfo(String siteId, GlobalManagerStatus roleType) {
            this.siteId = siteId;
            this.roleType = roleType;
            nodesInfo = new ArrayList<>();
        }

        SiteMsg convert2msg() {
            ArrayList<AphInfoMsg> aphInfoMsgs = new ArrayList<>();
            for (NodeInfo nodeInfo : nodesInfo) {
                aphInfoMsgs.add(nodeInfo.convert2msg());
            }

            SiteMsg siteMsg = SiteMsg.newBuilder().setId(siteId).setGmStatus(roleType).addAllAph(aphInfoMsgs).build();
            return siteMsg;
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
                        epoch = resp.getEpoch();
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
            return String.format("Cluster[%s] --- Nodes[%s]:  %s", getSiteId(), nodesInfo.size(), nodesInfo);
        }
    }

    @Data
    public static class NodeInfo {
        //AphInfoMsg aphInfo;

        GlobalManagerStatus roleType;

        @Getter
        String ipAddress;

        String corfuPortNum;

        String portNum;

        boolean leader;

        LogReplicationRuntime runtime;

        NodeInfo(String ipAddress, String portNum, GlobalManagerStatus roleType, String corfuPortNum) {
            this.leader = false;
            this.ipAddress = ipAddress;
            this.roleType = roleType;
            this.portNum = portNum;
            this.corfuPortNum = corfuPortNum;
        }

        public AphInfoMsg convert2msg() {
            AphInfoMsg aphInfoMsg = AphInfoMsg.newBuilder().setAddress(ipAddress).setPort(Integer.parseInt(portNum)).setCorfuPort(Integer.parseInt(corfuPortNum)).build();
            return aphInfoMsg;
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
}