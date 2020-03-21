package org.corfudb.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.util.NodeLocator;

import static org.corfudb.logreplication.infrastructure.CrossSiteConfiguration.RoleType.StandbySite;
import static org.corfudb.logreplication.infrastructure.CrossSiteConfiguration.RoleType.PrimarySite;

/**
 * This class represents the Replication Discovery Service.
 * It allows to discover all sites, acquire the lock and determine
 * the role of the current node: Source (sender) or Sink (receiver)
 */
@Slf4j
public class CorfuReplicationDiscoveryService implements Runnable {
    private final CorfuReplicationManager replicationManager;
    private String localEndpoint;

    public CorfuReplicationDiscoveryService(NodeLocator localEndpoint) {
        this.replicationManager = new CorfuReplicationManager();
        this.localEndpoint = localEndpoint.toEndpointUrl();
    }

    @Override
    public void run() {
        log.info("Initiate Corfu Replication Discovery");

        // Fetch Site Information (from Site Manager) = CrossSiteConfiguration
        CrossSiteConfiguration crossSiteConfig = fetchSiteConfiguration();

        // Get the current node information.
        CrossSiteConfiguration.NodeInfo nodeInfo = crossSiteConfig.getNodeInfo(localEndpoint);

        // Acquire lock and set it in the node information
        nodeInfo.setLeader(acquireLock());

        if (nodeInfo.isLeader()) {
            if (nodeInfo.getRoleType() == PrimarySite) {
                crossSiteConfig.getPrimarySite().setLeader(nodeInfo);
                log.info("Start as Source (sender/replicator) on node {}.", nodeInfo);
                replicationManager.setupReplicationLeaderRuntime(nodeInfo, crossSiteConfig);
                replicationManager.startLogReplication(crossSiteConfig);
                return;
            } else if (nodeInfo.getRoleType() == StandbySite) {
                // Standby Site
                // The LogReplicationServer (server handler) will initiate the SinkManager
                log.info("Start as Sink (receiver) on node {} ", nodeInfo);
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
