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

    public synchronized CrossSiteConfiguration fetchSiteConfiguration() throws IOException {
        if (crossSiteConfiguration == null) {
            crossSiteConfiguration = query();
        }
        return crossSiteConfiguration;
    }

    /**
     * Will be called when the site change and a new configuration is sent over
     * @param newConfiguration
     * @return
     */
    synchronized void update(CrossSiteConfiguration newConfiguration) {
        if (crossSiteConfiguration == null) {
            //If the the config hasn't been initialized, set the config
            crossSiteConfiguration = newConfiguration;
        } else if (newConfiguration.getEpoch() > crossSiteConfiguration.getEpoch()) {
            //If the newCongig has higher epoch, update it

            //TODO: enforce stop replication work and get into idle state
            //need to call disconnect to stop the current router?
            getCorfuReplicationDiscoveryService().getReplicationManager().stopLogReplication(crossSiteConfiguration);
            crossSiteConfiguration = newConfiguration;

            //System.out.print("\nnotify the site change");
            notifyAll();
        }
    }

    public synchronized CrossSiteConfiguration query() throws IOException {
        return null;
    };

    public abstract void start();
}
