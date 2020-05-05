package org.corfudb.logreplication.infrastructure;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.runtime.LogReplicationRuntime;
import org.corfudb.logreplication.runtime.LogReplicationRuntimeParameters;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class CrossSiteConfiguration {
    long epoch;
    @Getter
    SiteInfo primarySite;

    @Getter
    Map<String, SiteInfo> standbySites;

    public CrossSiteConfiguration(long epoch, SiteInfo primarySite, Map<String, SiteInfo> standbySites) {
        this.epoch = epoch;
        this.primarySite = primarySite;
        this.standbySites = standbySites;
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

    public static class SiteInfo {

        @Getter
        String siteId;

        RoleType roleType; //standby or active

        @Getter
        @Setter
        NodeInfo leader;

        @Getter
        List<NodeInfo> nodesInfo;

        public SiteInfo(SiteInfo info, RoleType roleType) {
            this.siteId = info.siteId;
            this.roleType = roleType;
            this.nodesInfo = new ArrayList<>(info.nodesInfo);
            for ( NodeInfo nodeInfo : info.nodesInfo) {
                NodeInfo newNode = new NodeInfo(nodeInfo.getIpAddress(), nodeInfo.getPortNum(), roleType, nodeInfo.getCorfuPortNum());
                this.nodesInfo.add(newNode);
            }
        }

        public SiteInfo(String siteId, RoleType roleType) {
            this.siteId = siteId;
            this.roleType = roleType;
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
    public static class NodeInfo {
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