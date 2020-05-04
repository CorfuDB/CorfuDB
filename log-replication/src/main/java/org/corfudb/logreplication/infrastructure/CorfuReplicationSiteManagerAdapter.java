package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.concurrent.Semaphore;

public abstract class CorfuReplicationSiteManagerAdapter {
    @Getter
    @Setter
    CorfuReplicationDiscoveryService corfuReplicationDiscoveryService;

    @Getter
    CrossSiteConfiguration crossSiteConfiguration;
    String localEndpoint;

    //For testing purpose to notify the change of siteConfig.
    @Getter
    Semaphore notification;

    public CrossSiteConfiguration fetchSiteConfiguration() throws IOException {
        if (crossSiteConfiguration != null) {
            return crossSiteConfiguration;
        } else {
            return update(query());
        }
    }

    synchronized CrossSiteConfiguration update(CrossSiteConfiguration newConfiguration) {
        if (crossSiteConfiguration == null) {
            crossSiteConfiguration = newConfiguration;

        } else if (newConfiguration.getPrimarySite().getSiteId() == crossSiteConfiguration.getPrimarySite().getSiteId() &&
                    newConfiguration.getNodeInfo(localEndpoint).getRoleType() == crossSiteConfiguration.getNodeInfo(localEndpoint).getRoleType()) {
            //If the primary doesn't change, and the current node's role type doesn't change do nothing
            return crossSiteConfiguration;
        } else {
            //TODO: enforce stop replication work and get into idle state
            //need to call disconnect to stop the current router?
            // for each runtime
            // logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.LOG_ENTRY_SYNC_REPLICATED,
            getCorfuReplicationDiscoveryService().getReplicationManager().stopLogReplication(crossSiteConfiguration);
            crossSiteConfiguration = newConfiguration;
            //notify the site change
            getCorfuReplicationDiscoveryService().notification.release();
            notification.release();
        }

        return crossSiteConfiguration;
    }

    public abstract CrossSiteConfiguration query() throws IOException;
    public abstract void start();
}
