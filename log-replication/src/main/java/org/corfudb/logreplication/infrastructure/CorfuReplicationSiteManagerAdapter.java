package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;

import java.io.IOException;

import static org.corfudb.logreplication.infrastructure.CorfuReplicationDiscoveryService.DiscoveryServiceEventType.DiscoverySite;

public abstract class CorfuReplicationSiteManagerAdapter {
    @Getter
    @Setter
    CorfuReplicationDiscoveryService corfuReplicationDiscoveryService;

    @Getter
    CrossSiteConfiguration crossSiteConfiguration;

    public synchronized CrossSiteConfiguration fetchSiteConfiguration() throws IOException {
        crossSiteConfiguration = query();
       return crossSiteConfiguration;
    }

    /**
     * Will be called when the site change and a new configuration is sent over
     * @param newConfiguration
     * @return
     */
    synchronized void update(CrossSiteConfiguration newConfiguration) {
            //System.out.print("\nnotify the site change");
            if (newConfiguration.getEpoch() > crossSiteConfiguration.getEpoch()) {
                crossSiteConfiguration = newConfiguration;
                corfuReplicationDiscoveryService.putEvent(new CorfuReplicationDiscoveryService.DiscoveryServiceEvent(DiscoverySite, newConfiguration));
            }
    }

    public CrossSiteConfiguration query() throws IOException {
        return null;
    };

    public abstract void start();
}
