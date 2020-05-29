package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.ServerContext;
import org.corfudb.logreplication.proto.LogReplicationSiteInfo.SiteStatus;
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

    /**
     * for site discovery service
     */
    @Getter
    private CorfuReplicationSiteManagerAdapter siteManager;


    /**
     * the current node's endpoint
     */
    private String localEndpoint;


    /**
     * the node's information
     */
    LogReplicationNodeInfo nodeInfo;

    boolean shouldRun = true;

    /**
     * Anny: Should be it unique?
     */
    private final UUID nodeId;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<DiscoveryServiceEvent> eventQueue = new LinkedBlockingQueue<>();

    public CorfuReplicationDiscoveryService(ServerContext serverContext, CorfuInterClusterReplicationServerNode serverNode,
                                            CorfuReplicationSiteManagerAdapter siteManager) {
        this.replicationServerNode = serverNode;
         this.siteManager = siteManager;
        this.siteManager.setCorfuReplicationDiscoveryService(this);

        //Anny: Does the getNodeID() give an unique id?
        this.nodeId = serverContext.getNodeId();

        CrossSiteConfiguration crossSiteConfig = siteManager.fetchSiteConfig();
        this.replicationManager = new CorfuReplicationManager(serverContext.getTransportType(), siteManager.fetchSiteConfig());
        this.localEndpoint = serverContext.getLocalEndpoint();
        this.nodeInfo = crossSiteConfig.getNodeInfo(localEndpoint);

        siteManager.start();
        registerToLogReplicationLock();
    }

    public void run() {
        while (shouldRun) {
            try {
                while (true) {
                    DiscoveryServiceEvent event = eventQueue.take();
                    processEvent(event);
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
                    LockListener logReplicationLockListener = new LogReplicationLockListener(this);
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

    /**
     * it is only called on the leader node and it triggers the log replication start
     */
    public void startLogReplication() {
        if (nodeInfo.isLeader() == false) {
            return;
        }

        // Update the siteEpoch metadata.
        replicationServerNode.getLogReplicationServer().getSinkManager().setSiteInfo(nodeInfo.getRoleType() == SiteStatus.ACTIVE ? true : false,
                replicationManager.getCrossSiteConfig().getSiteConfigID());

        log.debug("persit new siteConfigID " + replicationManager.getCrossSiteConfig().getSiteConfigID() + " status " +
                nodeInfo.getRoleType());

        if (nodeInfo.getRoleType() == SiteStatus.ACTIVE) {
            //crossSiteConfig.getPrimarySite().setLeader(nodeInfo);
            log.info("Start as Source (sender/replicator) on node {}.", nodeInfo);
            replicationManager.startLogReplication(nodeInfo, this);
        } else if (nodeInfo.getRoleType() == SiteStatus.STANDBY) {
            // Standby Site : the LogReplicationServer (server handler) will initiate the LogReplicationSinkManager

            log.info("Start as Sink (receiver) on node {} ", nodeInfo);
        }
    }

    public void stopLogReplication() {
        if (nodeInfo.isLeader() && nodeInfo.getRoleType() == SiteStatus.ACTIVE) {
            replicationManager.stopLogReplication();
        }
    }


    public void processLockAcquire() {
        log.debug("process lock acquire");
        replicationServerNode.getLogReplicationServer().getSinkManager().setLeader(true);

        // leader transition from true to true, do nothing;
        if (nodeInfo.isLeader()) {
            return;
        } else {
            // leader transition from false to true, start log replication.
            nodeInfo.setLeader(true);
            startLogReplication();
        }
    }

    /**
     * transition from false to false, do nothing
     * transition from true to false, stop replication.
     */
    public void processLockRelease() {
        replicationServerNode.getLogReplicationServer().getSinkManager().setLeader(false);

        if (nodeInfo.isLeader()) {
            stopLogReplication();
            nodeInfo.setLeader(false);
        }
    }


    public void processSiteFlip(CrossSiteConfiguration newConfig) {
        stopLogReplication();
        replicationManager.setCrossSiteConfig(newConfig);

        boolean leader = nodeInfo.isLeader();
        nodeInfo = newConfig.getNodeInfo(localEndpoint);
        nodeInfo.setLeader(leader);

        log.debug("new nodeinfo " + nodeInfo);
        startLogReplication();
    }

    public void processSiteChangeNotification(DiscoveryServiceEvent event) {
        //stale notification, skip
        if (event.getSiteConfigMsg().getSiteConfigID() < getReplicationManager().getCrossSiteConfig().getSiteConfigID()) {
            return;
        }

        CrossSiteConfiguration newConfig = siteManager.fetchSiteConfig();
        if (newConfig.getSiteConfigID() == getReplicationManager().getCrossSiteConfig().getSiteConfigID()) {
            if (nodeInfo.getRoleType() == SiteStatus.STANDBY) {
                return;
            }

            //If the current node it active, compare with the current siteConfig, see if there are addition/removal standbys
            getReplicationManager().processStandbyChange(nodeInfo, newConfig, this);
        } else {
            processSiteFlip(newConfig);
        }
    }

    /***
     * The standby site's leader change can lead to connection loss.
     * If the current node is not the active site's leader, discard the notification.
     * If the current node is the the active site's leader that is is responsible for the current
     * replication job, will restart the replication with the remote site.
     * @param event
     */
    private void processConnectionLossWithLeader(DiscoveryServiceEvent event) {

        if (!nodeInfo.isLeader())
            return;

        if (nodeInfo.getRoleType() != SiteStatus.ACTIVE) {
            return;
        }

        replicationManager.restartLogReplication(nodeInfo, event.getSiteID(), this);
    }

    /**
     * process event
     * @param event
     */
    public void processEvent(DiscoveryServiceEvent event) {
        switch (event.type) {
            case AcquireLock:
                processLockAcquire();
                break;

            case ReleaseLock:
                processLockRelease();
                break;

            case DiscoverySite:
                processSiteChangeNotification(event);
                break;

            case ConnectionLoss:
                processConnectionLossWithLeader(event);
                break;

            default:
                log.error("wrong event type {}", event);
        }
    }

    public synchronized void putEvent(DiscoveryServiceEvent event) {
        eventQueue.add(event);
        notifyAll();
    }
}
