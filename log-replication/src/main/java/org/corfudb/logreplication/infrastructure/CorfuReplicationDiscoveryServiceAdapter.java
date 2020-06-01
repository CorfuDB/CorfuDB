package org.corfudb.logreplication.infrastructure;

import org.corfudb.logreplication.proto.LogReplicationSiteInfo;

public interface CorfuReplicationDiscoveryServiceAdapter {

    /**
     *
     * @param crossSiteConfiguration
     */
    void updateSiteConfig(LogReplicationSiteInfo.SiteConfigurationMsg crossSiteConfiguration);

    /**
     *
     */
    void prepareSiteRoleChange();

    /**
     *
     * @return
     */
    int queryReplicationStatus();
}
