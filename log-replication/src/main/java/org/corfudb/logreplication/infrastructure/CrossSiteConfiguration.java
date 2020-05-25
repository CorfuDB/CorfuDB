package org.corfudb.logreplication.infrastructure;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.logreplication.proto.LogReplicationSiteInfo.AphInfoMsg;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.GlobalManagerStatus;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.SiteConfigurationMsg;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.SiteMsg;

import org.corfudb.infrastructure.logreplication.LogReplicationTransportType;
import org.corfudb.logreplication.runtime.CorfuLogReplicationRuntime;
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
                addStandbySite(siteInfo);
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

    public LogReplicationNodeInfo getNodeInfo(String endpoint) {
        List<SiteInfo> sites = new ArrayList<>(standbySites.values());

        sites.add(primarySite);
        LogReplicationNodeInfo nodeInfo = getNodeInfo(sites, endpoint);

        if (nodeInfo == null) {
            log.warn("No Site has node with IP {} ", endpoint);
        }

        return nodeInfo;
    }

    private LogReplicationNodeInfo getNodeInfo(List<SiteInfo> sitesInfo, String endpoint) {
        for(SiteInfo site : sitesInfo) {
            for (LogReplicationNodeInfo nodeInfo : site.getNodesInfo()) {
                if (nodeInfo.getEndpoint().equals(endpoint)) {
                    return nodeInfo;
                }
            }
        }

        log.warn("There is no nodeInfo for ipAddress {} ", endpoint);
        return null;
    }

    public void addStandbySite(SiteInfo siteInfo) {
        standbySites.put(siteInfo.getSiteId(), siteInfo);
    }

    public void removeStandbySite(String siteId) {
        standbySites.remove(siteId);
    }

    public static class SiteInfo {

        @Getter
        String siteId;

        @Getter
        GlobalManagerStatus roleType; //standby or active

        @Getter
        @Setter
        LogReplicationNodeInfo leader;

        @Getter
        List<LogReplicationNodeInfo> nodesInfo;

        public SiteInfo(SiteMsg siteMsg) {
            this.siteId = siteMsg.getId();
            this.roleType = siteMsg.getGmStatus();
            this.leader = null;
            this.nodesInfo = new ArrayList<>();
            for (AphInfoMsg aphInfoMsg : siteMsg.getAphList()) {
                LogReplicationNodeInfo newNode = new LogReplicationNodeInfo(aphInfoMsg.getAddress(),
                        Integer.toString(aphInfoMsg.getPort()), siteMsg.getGmStatus(), Integer.toString(aphInfoMsg.getCorfuPort()));
                this.nodesInfo.add(newNode);
            }
        }

        public SiteInfo(SiteInfo info, GlobalManagerStatus roleType) {
            this.siteId = info.siteId;
            this.roleType = roleType;
            this.leader = info.leader;
            this.nodesInfo = new ArrayList<>();
            for ( LogReplicationNodeInfo nodeInfo : info.nodesInfo) {
                LogReplicationNodeInfo newNode = new LogReplicationNodeInfo(nodeInfo.getIpAddress(), nodeInfo.getPortNum(), roleType, nodeInfo.corfuPortNum);
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
            for (LogReplicationNodeInfo nodeInfo : nodesInfo) {
                aphInfoMsgs.add(nodeInfo.convert2msg());
            }

            SiteMsg siteMsg = SiteMsg.newBuilder().setId(siteId).setGmStatus(roleType).addAllAph(aphInfoMsgs).build();
            return siteMsg;
        }


        public void connect(LogReplicationNodeInfo localNode, LogReplicationTransportType transport) {
            // TODO (Xiaoqin Ma): shouldn't it connect only to the lead node on the remote site?
            // It needs a runtime to do the negotiation with non leader remote too.

            for (LogReplicationNodeInfo nodeInfo : nodesInfo) {
                LogReplicationRuntimeParameters parameters = LogReplicationRuntimeParameters.builder()
                        .localCorfuEndpoint(localNode.getCorfuEndpoint())
                        .remoteLogReplicationServerEndpoint(nodeInfo.getEndpoint())
                        .transport(transport)
                        .build();
                CorfuLogReplicationRuntime replicationRuntime = new CorfuLogReplicationRuntime(parameters);
                replicationRuntime.connect();
                nodeInfo.setRuntime(replicationRuntime);
            }
        }

        /**
         * Retrieve Remote Leader Endpoint
         **
         * @return remote leader endpoint.
         */
        public LogReplicationNodeInfo getRemoteLeader() throws Exception {
            LogReplicationNodeInfo leaderNode = null;
            try {
                long epoch = -1;
                LogReplicationQueryLeaderShipResponse resp;
                for (LogReplicationNodeInfo nodeInfo : nodesInfo) {
                    resp = nodeInfo.runtime.queryLeadership();
                    if (resp.getEpoch() >= epoch && resp.isLeader()) {
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

}