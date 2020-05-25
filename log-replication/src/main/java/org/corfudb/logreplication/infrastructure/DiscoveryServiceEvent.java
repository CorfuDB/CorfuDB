package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo;

public class DiscoveryServiceEvent {
    DiscoveryServiceEventType type = null;
    @Getter
    LogReplicationSiteInfo.SiteConfigurationMsg siteConfigMsg = null;

    @Getter
    @Setter
    String siteID;

    DiscoveryServiceEvent(DiscoveryServiceEventType type, LogReplicationSiteInfo.SiteConfigurationMsg siteConfigMsg) {
        this.type = type;
        this.siteConfigMsg = siteConfigMsg;
    }

    DiscoveryServiceEvent(DiscoveryServiceEventType type, String siteID, LogReplicationSiteInfo.SiteConfigurationMsg siteConfigMsg) {
        this.type = type;
        this.siteID = siteID;
        this.siteConfigMsg = siteConfigMsg;
    }

    public enum DiscoveryServiceEventType {
        DiscoverySite("SiteChange"),
        AcquireLock("AcquireLock"),
        ReleaseLock("ReleaseLock"),
        ConnectionLossWithLeader( "ConnectionLossWithLeader");

        @Getter
        String val;
        DiscoveryServiceEventType(String newVal) {
            val = newVal;
        }
    }
}
