package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.logreplication.proto.LogReplicationSiteInfo.GlobalManagerStatus;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.SiteConfigurationMsg;

import org.corfudb.infrastructure.LogReplicationTransportType;

import java.util.concurrent.LinkedBlockingQueue;

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

    private final LogReplicationTransportType transport;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<DiscoveryServiceEvent> eventQueue = new LinkedBlockingQueue<>();

    public CorfuReplicationDiscoveryService(String endpoint, CorfuReplicationServerNode serverNode, CorfuReplicationSiteManagerAdapter siteManager, LogReplicationTransportType transport) {
        this.replicationServerNode = serverNode;
        this.replicationManager = new CorfuReplicationManager();
        this.localEndpoint = endpoint;
        this.siteManager = siteManager;
        this.siteManager.setCorfuReplicationDiscoveryService(this);
        this.transport = transport;
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
                        if (event.siteConfigMsg.getEpoch() > crossSiteConfig.getEpoch()) {
                            break;
                        }
                    }
                    replicationManager.stopLogReplication(crossSiteConfig);
                }
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
            log.info("Running Corfu Replication Discovery Service");

            // Fetch Site Information (from Site Manager) = CrossSiteConfiguration
            crossSiteConfig = siteManager.fetchSiteConfig();
            //System.out.print("\n Primary Site " + crossSiteConfig.getPrimarySite());

            // Get the current node information.
            nodeInfo = crossSiteConfig.getNodeInfo(localEndpoint);

            // Acquire lock and set it in the node information
            nodeInfo.setLeader(acquireLock());

            if (nodeInfo.isLeader()) {
                if (nodeInfo.getRoleType() == GlobalManagerStatus.ACTIVE) {

                    crossSiteConfig.getPrimarySite().setLeader(nodeInfo);
                    log.info("Start as Source (sender/replicator) on node {}.", nodeInfo);
                    // TODO(Anny): parallel? or not really, cause each site should have it's own state machine?
                    for (CrossSiteConfiguration.SiteInfo remoteSite : crossSiteConfig.getStandbySites().values()) {
                        try {
                            replicationManager.connect(nodeInfo, remoteSite, transport);
                            replicationManager.startLogReplication(remoteSite.siteId, crossSiteConfig);
                        } catch (Exception e) {
                            log.error("Failed to start log replication to remote site {}", remoteSite.getSiteId());
                        }
                    }
                } else if (nodeInfo.getRoleType() == GlobalManagerStatus.STANDBY) {
                    // Standby Site
                    // The LogReplicationServer (server handler) will initiate the SinkManager
                    // Update the siteEpoch metadata.
                    replicationServerNode.getLogReplicationServer().getSinkManager().getPersistedWriterMetadata().
                            setupEpoch(crossSiteConfig.getEpoch());
                    log.info("Start as Sink (receiver) on node {} ", nodeInfo);
                    //System.out.print("\nStart as Sink (receiver) on node " + nodeInfo + " siteConig " + crossSiteConfig);
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
        //}
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
        // TODO(Anny): Add Distributed Lock logic here
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
        SiteConfigurationMsg siteConfigMsg;

        DiscoveryServiceEvent(DiscoveryServiceEventType type, SiteConfigurationMsg siteConfigMsg) {
            this.type = type;
            this.siteConfigMsg = siteConfigMsg;
        }
    }
}
