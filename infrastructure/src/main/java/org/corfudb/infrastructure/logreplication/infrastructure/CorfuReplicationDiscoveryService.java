package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.CorfuReplicationClusterManagerAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
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
 * - Cluster (site) discovery (active and standby)
 * - Lock Acquisition (leader election)
 * - Log Replication Node role: Source (sender) or Sink (receiver)
 */
@Slf4j
public class CorfuReplicationDiscoveryService implements Runnable, CorfuReplicationDiscoveryServiceAdapter {

    /**
     * Used by both active cluster and standby cluster.
     */
    private final CorfuInterClusterReplicationServerNode replicationServerNode;

    /**
     * Bookkeeping the topologyConfigId, version number and other log replication state information.
     * It is backed by a corfu store table.
     */
    @Getter
    private final LogReplicationMetadataManager logReplicationMetadataManager;

    /**
     * Lock-related configuration parameters
     */
    private static final String LOCK_GROUP = "Log_Replication_Group";
    private static final String LOCK_NAME = "Log_Replication_Lock";

    /**
     * Used by the active cluster
     */
    @Getter
    private CorfuReplicationManager replicationManager;

    /**
     * Adapter for cluster discovery service
     */
    @Getter
    private CorfuReplicationClusterManagerAdapter clusterManager;

    /**
     * Current node's endpoint
     */
    private String localEndpoint;

    /**
     * Current node information
     */
    NodeDescriptor localNodeInfo;

    /**
     * Unique node identifier
     */
    private final UUID nodeId;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<DiscoveryServiceEvent> eventQueue = new LinkedBlockingQueue<>();

    /**
     * Defines the topology of the multi-cluster setting, which is discovered through the Cluster Manager
     */
    private TopologyDescriptor topologyDescriptor;

    private String pluginFilePath;

    boolean shouldRun = true;

    /**
     * Constructor Discovery Service
     *
     * @param serverContext
     * @param serverNode
     * @param clusterManager
     */
    public CorfuReplicationDiscoveryService(ServerContext serverContext, CorfuInterClusterReplicationServerNode serverNode,
                                            CorfuReplicationClusterManagerAdapter clusterManager) {
        this.replicationServerNode = serverNode;
        this.clusterManager = clusterManager;
        this.clusterManager.setCorfuReplicationDiscoveryService(this);
        this.nodeId = serverContext.getNodeId();
        this.localEndpoint = serverContext.getLocalEndpoint();
        this.pluginFilePath = serverContext.getPluginConfigFilePath();
        this.topologyDescriptor = new TopologyDescriptor(clusterManager.queryTopologyConfig());
        this.localNodeInfo = topologyDescriptor.getNodeInfo(localEndpoint);
        this.logReplicationMetadataManager = serverNode.getLogReplicationServer().getSinkManager().getLogReplicationMetadataManager();

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
                log.error("Caught an exception. Stop discovery service.", e);
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
                    String corfuEndpoint = NodeLocator.parseString(localEndpoint).getHost() + ":" + localNodeInfo.getCorfuPort();
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
     * This method is only called on the leader node and it triggers the start of log replication
     *
     * Depending on the role of the cluster to which this leader node belongs to, it will start
     * as source (sender/producer) or sink (receiver).
     */
    private void startLogReplication() {
        if (!localNodeInfo.isLeader()) {
            log.warn("Current node {} is not the lead node, log replication cannot be started.", localEndpoint);
            return;
        }

        boolean activeCluster = localNodeInfo.getRoleType() == ClusterRole.ACTIVE;

        updateTopologyConfigId(activeCluster);

        if (activeCluster) {
            log.info("Start as Source (sender/replicator) on node {}.", localNodeInfo);
            replicationManager = new CorfuReplicationManager(topologyDescriptor, replicationServerNode.getLogReplicationConfig(),
                    localNodeInfo, logReplicationMetadataManager, pluginFilePath);
            replicationManager.start();
        } else if (localNodeInfo.getRoleType() == ClusterRole.STANDBY) {
            // Standby Site : the LogReplicationServer (server handler) will initiate the LogReplicationSinkManager
            log.info("Start as Sink (receiver) on node {} ", localNodeInfo);
        } else {
            log.error("Log Replication not started on this cluster. Leader node {} belongs to cluster with {} role.",
                    localEndpoint, localNodeInfo.getRoleType());
        }
    }

    private void updateTopologyConfigId(boolean active) {
        // TODO (Xiaoqin Ma): can you please add some info on why this is needed.
        replicationServerNode.getLogReplicationServer().getSinkManager()
                .updateSiteConfigID(active, topologyDescriptor.getTopologyConfigId());

        log.debug("Persist new topologyConfigId {}, status={}", topologyDescriptor.getTopologyConfigId(),
                localNodeInfo.getRoleType());
    }

    /**
     * Stop ongoing Log Replication
     */
    private void stopLogReplication() {
        if (localNodeInfo.isLeader() && localNodeInfo.getRoleType() == ClusterRole.ACTIVE) {
            replicationManager.stop();
        }
    }

    /**
     * Process lock acquisition event
     */
    public void processLockAcquire() {
        log.info("Process lock acquire event");
        replicationServerNode.getLogReplicationServer().getSinkManager().setLeader(true);

        if (!localNodeInfo.isLeader()) {
            // leader transition from false to true, start log replication.
            localNodeInfo.setLeader(true);
            startLogReplication();
        }
    }

    /**
     * Process lock release event
     *
     * Set leadership metadata and stop log replication in the event of leadership loss
     */
    public void processLockRelease() {
        replicationServerNode.getLogReplicationServer().getSinkManager().setLeader(false);

        if (localNodeInfo.isLeader()) {
            stopLogReplication();
            localNodeInfo.setLeader(false);
        }
    }


    public void processSiteFlip(TopologyDescriptor newConfig) {
        stopLogReplication();
        //TODO pankti: read the configuration again and refresh the LogReplicationConfig object
        replicationManager.setTopologyDescriptor(newConfig);
        startLogReplication();
    }

    public void processSiteChangeNotification(DiscoveryServiceEvent event) {
        // Stale notification, skip
        if (event.getTopologyConfig().getTopologyConfigID() < getReplicationManager().getTopologyDescriptor().getTopologyConfigId()) {
            return;
        }

        TopologyDescriptor newConfig = new TopologyDescriptor(clusterManager.queryTopologyConfig());
        if (newConfig.getTopologyConfigId() == getReplicationManager().getTopologyDescriptor().getTopologyConfigId()) {
            if (localNodeInfo.getRoleType() == ClusterRole.STANDBY) {
                return;
            }

            // If the current node is active, compare with the current siteConfig, see if there are addition/removal standbys
            getReplicationManager().processStandbyChange(newConfig);
        } else {
            processSiteFlip(newConfig);
        }
    }

    /***
     * The standby cluster's leader change can lead to connection loss.
     * If the current node is not the active cluster's leader, discard the notification.
     * If the current node is the the active cluster's leader that is is responsible for the current
     * replication job, will restart the replication with the remote cluster.
     *
     * @param event
     */
    private void processConnectionLoss(DiscoveryServiceEvent event) {

        if (!localNodeInfo.isLeader() || localNodeInfo.getRoleType() != ClusterRole.ACTIVE) {
            return;
        }

        replicationManager.restart(event.getRemoteSiteInfo());
    }

    /***
     * After an upgrade, the active site should perform a snapshot sync
     */
    private void processUpgrade(DiscoveryServiceEvent event) {
        if (localNodeInfo.isLeader() && localNodeInfo.getRoleType() == ClusterRole.ACTIVE) {
            // TODO pankti: is this correct?
            replicationManager.restart(event.getRemoteSiteInfo());
        }
    }

    /**
     * Process event
     */
    public void processEvent(DiscoveryServiceEvent event) {
        switch (event.type) {
            case ACQUIRE_LOCK:
                processLockAcquire();
                break;

            case RELEASE_LOCK:
                processLockRelease();
                break;

            case DISCOVERY_SITE:
                processSiteChangeNotification(event);
                break;

            case UPGRADE:
                processUpgrade(event);
                break;

            default:
                log.error("wrong event type {}", event);
        }
    }

    public synchronized void putEvent(DiscoveryServiceEvent event) {
        eventQueue.add(event);
        notifyAll();
    }

    @Override
    public void updateSiteConfig(LogReplicationClusterInfo.TopologyConfigurationMsg topologyConfig) {
        putEvent(new DiscoveryServiceEvent(DiscoveryServiceEvent.DiscoveryServiceEventType.DISCOVERY_SITE, topologyConfig));
    }

    /**
     * Query the current all replication stream log tail and remeber the max
     * and query each standbySite information according to the ackInformation decide all manay total
     * msg needs to send out.
     */
    @Override
    public void prepareSiteRoleChange() {
        replicationManager.prepareSiteRoleChange();
    }

    /**
     * Query the current all replication stream log tail and calculate the number of messages to be sent.
     * If the max tail has changed, give 0%. Otherwise,
     */
    @Override
    public int queryReplicationStatus() {
        return replicationManager.queryReplicationStatus();
    }

    public void shutdown() {
        if (replicationManager != null) {
            replicationManager.stop();
        }
    }
}
