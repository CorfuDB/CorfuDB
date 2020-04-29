package org.corfudb.logreplication.infrastructure;

import java.io.IOException;

public abstract class CorfuReplicationSiteManagerAdapter {
    CrossSiteConfiguration crossSiteConfiguration;
    String localEndpoint;

    public CrossSiteConfiguration fetchSiteConfiguration() throws IOException {
        if (crossSiteConfiguration != null) {
            return crossSiteConfiguration;
        } else {
            return query();
        }
    }

    void update(CrossSiteConfiguration newConfiguration) {
        //If the primary doesn't change, and the current node's role type doesn't change do nothing
        if (newConfiguration.getPrimarySite().getSiteId() == crossSiteConfiguration.getPrimarySite().getSiteId() &&
                newConfiguration.getNodeInfo(localEndpoint).getRoleType() == crossSiteConfiguration.getNodeInfo(localEndpoint).getRoleType()) {
            return;
        } else {
            //TODO: enforce stop replication work and get into idle state
            //need to call disconnect to stop the current router?
            crossSiteConfiguration = newConfiguration;
            //TODO: redo site discovery;
        }
    }


    public abstract CrossSiteConfiguration query() throws IOException;
    public abstract void siteChangeNotification() throws IOException;
}
