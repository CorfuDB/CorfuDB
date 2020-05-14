package org.corfudb.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationTransportType;
import org.corfudb.util.NodeLocator;

import static org.corfudb.logreplication.infrastructure.CrossSiteConfiguration.RoleType.StandbySite;
import static org.corfudb.logreplication.infrastructure.CrossSiteConfiguration.RoleType.PrimarySite;

/**
 * This class represents the Replication Discovery Service.
 *
 * It manages the following:
 * - Site discovery (active and standby)
 * - Lock Acquisition (leader election)
 * - Log Replication Node role: Source (sender) or Sink (receiver)
 */
@Slf4j
public class CorfuReplicationDiscoveryService implements Runnable {

    private final CorfuReplicationManager replicationManager;
    private final String localEndpoint;
    private final LogReplicationTransportType transport;

    public CorfuReplicationDiscoveryService(NodeLocator localEndpoint, LogReplicationTransportType transport) {
        this.replicationManager = new CorfuReplicationManager();
        this.localEndpoint = localEndpoint.toEndpointUrl();
        this.transport = transport;
    }

    @Override
    public void run() {
        // TODO (Xiaoqin Ma): This is running in a continous loop, shouldn't it only run once and then on site change?
        // while (true) {
            try {
                log.info("Running Corfu Replication Discovery Service");

                // Fetch Site Information (from Site Manager) = CrossSiteConfiguration
                CrossSiteConfiguration crossSiteConfig = fetchSiteConfiguration();

                // Get the current node information.
                CrossSiteConfiguration.NodeInfo currentNodeInfo = crossSiteConfig.getNodeInfo(localEndpoint);

                // Acquire lock and set it in the node information
                currentNodeInfo.setLeader(acquireLock());

                if (currentNodeInfo.isLeader()) {
                    if (currentNodeInfo.getRoleType() == PrimarySite) {
                        crossSiteConfig.getPrimarySite().setLeader(currentNodeInfo);
                        log.info("Start as Source (sender/replicator)");
                        // TODO(Anny): parallel? or not really, cause each site should have it's own state machine?
                        for (CrossSiteConfiguration.Site remoteSite : crossSiteConfig.getStandbySites().values()) {
                            try {
                                replicationManager.connect(currentNodeInfo, remoteSite, transport);
                                replicationManager.startLogReplication(remoteSite.siteId);
                            } catch (Exception e) {
                                log.error("Failed to start log replication to remote site {}", remoteSite.getSiteId());
                            }
                        }
                    } else if (currentNodeInfo.getRoleType() == StandbySite) {
                        // The LogReplicationServer (server handler) will initiate the SinkManager
                        log.info("Start as Sink (receiver)");
                    }
                } else {
                    log.info("Log Replication acquired by another node.");
                }

                // Todo: Re-schedule periodically, attempt to acquire lock
                // This class should keep state and re-schedule discovery,
                // if nothing has changed nothing is done, if
                // something changes it should stop previous replication.
            } catch (Exception e) {
                log.error("Caught Exception while attempting to replicate the log, retry. ", e);
            }
        //}
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
        // TODO(Anny): Add Distributed Lock logic here
        return true;
    }
}
