package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.ServerContext;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.GlobalManagerStatus;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.SiteConfigurationMsg;
import org.corfudb.infrastructure.logreplication.LogReplicationTransportType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.utils.lock.LockClient;
import org.corfudb.utils.lock.LockListener;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;

import java.util.UUID;
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
    private final CorfuInterClusterReplicationServerNode replicationServerNode;

    /**
     * Lock-related configuration parameters
     */
    private static final String LOCK_GROUP = "Log_Replication_Group";
    private static final String LOCK_NAME = "Log_Replication_Lock";

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
    private final UUID nodeId;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<DiscoveryServiceEvent> eventQueue = new LinkedBlockingQueue<>();

    public CorfuReplicationDiscoveryService(ServerContext serverContext, CorfuInterClusterReplicationServerNode serverNode,
                                            CorfuReplicationSiteManagerAdapter siteManager) {
        this.replicationServerNode = serverNode;
        this.replicationManager = new CorfuReplicationManager();
        this.localEndpoint = serverContext.getLocalEndpoint();
        this.siteManager = siteManager;
        this.siteManager.setCorfuReplicationDiscoveryService(this);
        this.transport = serverContext.getTransportType();
        this.nodeId = serverContext.getNodeId();
    }

    public void run() {

        registerToLogReplicationLock();

        //TODO: @maxi from this point on, probably all this code should be somehow moved to
        // the callbacks on LogReplicationLockListener class.

        siteManager.start();

        while (shouldRun) {
            try {

                // Discover the current site configuration.
                runService();

                // blocking on the event queue.
                // will unblock until there is a new epoch for site information.
                synchronized (eventQueue) {
                    while (true) {
                        // TODO(maxi): more events (Johnny's side) - No DIFF of the configuration
                        DiscoveryServiceEvent event = eventQueue.take();
                        if (event.siteConfigMsg.getEpoch() > crossSiteConfig.getEpoch()) {
                            break;
                        }
                    }
                    // TODO(Xiaoqin Ma) revisit this, depending on the configuration change
                    // it could be a new standby is added, comes back... or maybe switch
                    // processSiteChange(event.siteConfigMsg); No diff available, we need to find the changes...
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

    /**
     * Register interest on Log Replication Lock.
     *
     * The node that acquires the lock will drive/lead log replication.
     */
    private void registerToLogReplicationLock() {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    // TODO(Anny): this is a hack for our local tests to work, in production it will be always configured
                    // to port 9000 (or read from a file)
                    String corfuPort = localEndpoint.equals("localhost:9020") ? ":9001" : ":9000";
                    String corfuEndpoint = NodeLocator.parseString(localEndpoint).getHost() + corfuPort;
                    CorfuRuntime runtime = CorfuRuntime.fromParameters(CorfuRuntimeParameters.builder().build())
                            .parseConfigurationString(corfuEndpoint)
                            .connect();
                    LockClient lock = new LockClient(nodeId, runtime);
                    // Callback on lock acquisition or revoke
                    LockListener logReplicationLockListener = new LogReplicationLockListener();
                    // Register Interest on the shared Log Replication Lock
                    lock.registerInterest(LOCK_GROUP, LOCK_NAME, logReplicationLockListener);
                } catch (Exception e) {
                    log.error("Error while attempting to register interest on log replication lock {}:{}", LOCK_GROUP, LOCK_NAME, e);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to register interest on log replication lock.", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    public void runService() {
        try {
            log.info("Running Corfu Replication Discovery Service");

            // TODO: move logic once acquired lock to the lock listener...
            // Fetch Site Information (from Site Manager) = CrossSiteConfiguration
            crossSiteConfig = siteManager.fetchSiteConfig();

            // Get the current node information.
            nodeInfo = crossSiteConfig.getNodeInfo(localEndpoint);

            // Acquire lock and set it in the node information
            // TODO: remove this
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
                            // TODO (if failed): put logic..
                            // If failed against a standby, retry..
                        }
                    }
                } else if (nodeInfo.getRoleType() == GlobalManagerStatus.STANDBY) {
                    // Standby Site
                    // The LogReplicationServer (server handler) will initiate the LogReplicationSinkManager
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
