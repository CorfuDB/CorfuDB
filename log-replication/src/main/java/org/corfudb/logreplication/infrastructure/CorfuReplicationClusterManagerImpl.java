package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.SiteConfigurationMsg;


public class CorfuReplicationClusterManagerImpl implements CorfuReplicationClusterManagerAdapter {
    @Getter
    CorfuReplicationDiscoveryServiceAdapter corfuReplicationDiscoveryService;

    @Getter
    SiteConfigurationMsg siteConfigMsg;

    public void setCorfuReplicationDiscoveryService(CorfuReplicationDiscoveryServiceAdapter corfuReplicationDiscoveryService) {
        this.corfuReplicationDiscoveryService = corfuReplicationDiscoveryService;
        start();
    }

    public synchronized SiteConfigurationMsg fetchSiteConfig() {
        siteConfigMsg = querySiteConfig();
        return siteConfigMsg;
    }

    /**
     * Will be called when the site change and a new configuration is sent over
     * @param newSiteConfigMsg
     * @return
     */
    public synchronized void updateSiteConfig(SiteConfigurationMsg newSiteConfigMsg) {
            if (newSiteConfigMsg.getSiteConfigID() > siteConfigMsg.getSiteConfigID()) {
                siteConfigMsg = newSiteConfigMsg;
                corfuReplicationDiscoveryService.updateSiteConfig(siteConfigMsg);
            }
    }

    public void prepareSiteRoleChange() {
        corfuReplicationDiscoveryService.prepareSiteRoleChange();
    }

    public int queryReplicationStatus() {
        return corfuReplicationDiscoveryService.queryReplicationStatus();
    }

    //TODO: handle the case that querySiteConfig return an exception.
    public SiteConfigurationMsg querySiteConfig() { return null;};

    public void start() {};

    public void shutdown() {};
}
