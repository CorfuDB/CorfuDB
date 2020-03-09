package org.corfudb.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.util.NodeLocator;

/**
 * This class represents the Replication Discovery Service.
 * It allows to discover all sites, acquire the lock and determine
 * the role of the current node: Source (sender) or Sink (receiver)
 */
@Slf4j
public class CorfuReplicationDiscoveryService implements Runnable {

    private final CorfuReplicationManager replicationManager;

    private String port;

    public CorfuReplicationDiscoveryService(NodeLocator localEndpoint) {
        this.replicationManager = new CorfuReplicationManager();
        // Todo (remove) this is temporal (while site manager info logic is implemented)
        this.port = Integer.toString(localEndpoint.getPort());
    }

    @Override
    public void run() {
        log.info("Initiate Corfu Replication Discovery");

        // (1) Try to acquire the lock (Medhavi/Srinivas)
        boolean lockAcquired = acquireLock();

        // If lock is acquired by this node
        if (lockAcquired) {
            // (2) Initiate discovery protocol:
            //      - Fetch Site Information (from Site Manager) = CrossSiteConfiguration
            CrossSiteConfiguration crossSiteConfig = fetchSiteConfiguration();

            // Todo: remove port comparison, temp for testing
            if (crossSiteConfig.isLocalSource() && port.equals("9010")) {
                // If current node is part of Primary Site (source) start log replication
                log.info("Start as Source (sender/replicator).");
                replicationManager.startLogReplication(crossSiteConfig);
            } else {
                // Standby Site
                // The LogReplicationServer (server handler) will initiate the SinkManager
                log.info("Start as Sink (receiver)");
            }
        }

        // Todo: Re-schedule periodically, attempt to acquire lock

        // This class should keep state and re-schedule discovery,
        // if nothing has changed nothing is done, if
        // something changes it should stop previous replication.
    }

    /**
     * Fetch Sites Configuration.
     *
     * @return cross-site configuration.
     */
    private CrossSiteConfiguration fetchSiteConfiguration() {
        return new CrossSiteConfiguration();
    }

    /**
     * Attempt to acquire lock, to become the lead replication node of this cluster.
     *
     * @return True if lock has been acquired by this node. False, otherwise.
     */
    private boolean acquireLock() {
        return true;
    }
}
