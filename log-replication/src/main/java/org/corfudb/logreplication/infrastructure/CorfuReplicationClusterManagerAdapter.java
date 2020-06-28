package org.corfudb.logreplication.infrastructure;

import org.corfudb.logreplication.proto.LogReplicationSiteInfo;

public interface CorfuReplicationClusterManagerAdapter {

    public void setCorfuReplicationDiscoveryService(CorfuReplicationDiscoveryServiceAdapter corfuReplicationDiscoveryService);

    // Reference implemenation in CorfuReplicationSiteManagerImpl
    LogReplicationSiteInfo.SiteConfigurationMsg fetchSiteConfig();

    // This will call the API to query the siteInfos.
    LogReplicationSiteInfo.SiteConfigurationMsg querySiteConfig();

    // This is called when get a notification of site config change.
    void updateSiteConfig(LogReplicationSiteInfo.SiteConfigurationMsg newSiteConfigMsg);

    void start();

    void shutdown();

    void prepareSiteRoleChange();

    int queryReplicationStatus();
}
