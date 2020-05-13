package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.LinkedBlockingQueue;

import static org.corfudb.logreplication.infrastructure.CrossSiteConfiguration.RoleType.PrimarySite;
import static org.corfudb.logreplication.infrastructure.CrossSiteConfiguration.RoleType.StandbySite;

/**
 * This class represents the Replication Discovery Service.
 * It allows to discover all sites, acquire the lock and determine
 * the role of the current node: Source (sender) or Sink (receiver)
 */
@Slf4j
public class CorfuReplicationDiscoveryService implements Runnable {

    /**
     * Used by both primary site and standby site.
     */
    private final CorfuReplicationServerNode replicationServerNode;

    /**
     * Used by the primary site
     */
    @Getter
    private final CorfuReplicationManager replicationManager;
    private CorfuReplicationSiteManagerAdapter siteManager;
    private String localEndpoint;
    boolean shouldRun = true;
    CrossSiteConfiguration crossSiteConfig;
    CrossSiteConfiguration.NodeInfo nodeInfo = null;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<DiscoveryServiceEvent> eventQueue = new LinkedBlockingQueue<>();

    public CorfuReplicationDiscoveryService(String endpoint, CorfuReplicationServerNode serverNode, CorfuReplicationSiteManagerAdapter siteManager) {
        this.replicationServerNode = serverNode;
        this.replicationManager = new CorfuReplicationManager();
        this.localEndpoint = endpoint;
        this.siteManager = siteManager;
        this.siteManager.setCorfuReplicationDiscoveryService(this);
    }

    public void run() {
        siteManager.start();

        while (shouldRun) {
            try {
                //discover the current site configuration.
                runService();

                // blocking on the event queue.
                // will unblock untill there is a new epoch for site information.
                synchronized (eventQueue) {
                    while (true) {
                        DiscoveryServiceEvent event = eventQueue.take();
                        if (event.crossSiteConfiguration.getEpoch() > crossSiteConfig.getEpoch()) {
                            break;
                        }
                    }
                }
                replicationManager.stopLogReplication(crossSiteConfig);
            } catch (Exception e) {
                log.error("caught an exception ", e);
                shouldRun = false;
                if (e instanceof InterruptedException) {
                    Thread.interrupted();
                }
            }
        }
    }

    public void runService() {
        try {
            log.info("Run Corfu Replication Discovery");
            //System.out.print("\nRun Corfu Replication Discovery Service");

            // Fetch Site Information (from Site Manager) = CrossSiteConfiguration
            crossSiteConfig = siteManager.fetchSiteConfiguration();
            //System.out.print("\n Primary Site " + crossSiteConfig.getPrimarySite());

            // Get the current node information.
            nodeInfo = crossSiteConfig.getNodeInfo(localEndpoint);

            // Acquire lock and set it in the node information
            nodeInfo.setLeader(acquireLock());

            if (nodeInfo.isLeader()) {
                if (nodeInfo.getRoleType() == PrimarySite) {
                    crossSiteConfig.getPrimarySite().setLeader(nodeInfo);
                    log.info("Start as Source (sender/replicator) on node {}.", nodeInfo);
                    try {
                        replicationManager.setupReplicationLeaderRuntime(nodeInfo, crossSiteConfig);
                    } catch (InterruptedException ie) {
                        log.error("Corfu Replication Discovery Service is interrupted", ie);
                        throw ie;
                    }
                    replicationManager.startLogReplication(crossSiteConfig);

                    return;
                } else if (nodeInfo.getRoleType() == StandbySite) {
                    // Standby Site
                    // The LogReplicationServer (server handler) will initiate the SinkManager
                    // Update the siteEpoch metadata.
                    replicationServerNode.getLogReplicationServer().getSinkManager().getPersistedWriterMetadata().
                            setupEpoch(crossSiteConfig.getEpoch());
                    log.info("Start as Sink (receiver) on node {} ", nodeInfo);
                }
            }
            // Todo: Re-schedule periodically, attempt to acquire lock

        } catch (Exception e) {
                log.error("Caught Exception while discovering remote sites, retry. ", e);
        } finally {
            if (nodeInfo != null && nodeInfo.isLeader()) {
                    releaseLock();
            }
        }
    }

    public synchronized void putEvent(DiscoveryServiceEvent event) {
        eventQueue.add(event);
        notifyAll();
    }

    /**
     * Attempt to acquire lock, to become the lead replication node of this cluster.
     *
     * @return True if lock has been acquired by this node. False, otherwise.
     */
    private boolean acquireLock() {
        return true;
    }

    private void releaseLock() {

    }

    public enum DiscoveryServiceEventType {
        DiscoverySite("SiteChange");

        @Getter
        String val;
        DiscoveryServiceEventType(String newVal) {
            val = newVal;
        }
    }

    static class DiscoveryServiceEvent {
        DiscoveryServiceEventType type;
        @Getter
        CrossSiteConfiguration crossSiteConfiguration;

        DiscoveryServiceEvent(DiscoveryServiceEventType type, CrossSiteConfiguration config) {
            this.type = type;
            this.crossSiteConfiguration = config;
        }
    }
}
