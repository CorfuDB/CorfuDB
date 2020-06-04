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

    public DiscoveryServiceEvent(DiscoveryServiceEventType type) {
       this.type = type;
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, String siteID) {
        new DiscoveryServiceEvent(type);
        this.siteID = siteID;
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, LogReplicationSiteInfo.SiteConfigurationMsg siteConfigMsg) {
        this.type = type;
        this.siteConfigMsg = siteConfigMsg;
    }

    public enum DiscoveryServiceEventType {
        DiscoverySite("SiteChange"),
        AcquireLock("AcquireLock"),
        ReleaseLock("ReleaseLock"),
        ConnectionLoss( "ConnectionLoss");

        @Getter
        String description;

        DiscoveryServiceEventType(String description) {
            this.description = description;
        }
    }
}
