package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo;
import org.corfudb.logreplication.runtime.CorfuLogReplicationRuntime;


@Slf4j
class LogReplicationNodeInfo {

    @Setter
    @Getter
    LogReplicationSiteInfo.SiteStatus roleType;

    @Getter
    String ipAddress;

    @Setter
    @Getter
    String corfuPortNum;

    @Getter
    String portNum;

    @Getter
    @Setter
    boolean leader;

    @Getter
    @Setter
    CorfuLogReplicationRuntime runtime;

    LogReplicationNodeInfo(String ipAddress, String portNum, LogReplicationSiteInfo.SiteStatus roleType, String corfuPortNum) {
        this.leader = false;
        this.ipAddress = ipAddress;
        this.roleType = roleType;
        this.portNum = portNum;
        this.corfuPortNum = corfuPortNum;
    }

    public LogReplicationSiteInfo.NodeInfoMsg convert2msg() {
        LogReplicationSiteInfo.NodeInfoMsg aphInfoMsg = LogReplicationSiteInfo.NodeInfoMsg.newBuilder().setAddress(ipAddress).setPort(Integer.parseInt(portNum)).setCorfuPort(Integer.parseInt(corfuPortNum)).build();
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
