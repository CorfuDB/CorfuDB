package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.LogReplicationServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.CorfuReplicationClusterManagerAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;

import org.corfudb.infrastructure.logreplication.utils.LogReplicationStreamNameTableManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.utils.lock.LockClient;
import org.corfudb.utils.lock.LockListener;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
     * Bookkeeping the topologyConfigId, version number and other log replication state information.
     * It is backed by a corfu store table.
     **/
    @Getter
    private LogReplicationMetadataManager logReplicationMetadataManager;

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
     * Defines the topology of the multi-cluster setting, which is discovered through the Cluster Manager
     */
    private TopologyDescriptor topologyDescriptor;

    /**
     * Defines the cluster to which this node belongs to.
     */
    private ClusterDescriptor localClusterDescriptor;

    /**
     * Current node's endpoint
     */
    private String localEndpoint;

    /**
     * Current node information
     */
    NodeDescriptor localNodeDescriptor;

    /**
     * Unique node identifier
     */
    private final UUID nodeId;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<DiscoveryServiceEvent> eventQueue = new LinkedBlockingQueue<>();

    private CompletableFuture<LogReplicationContext> discoveryCallback;

    private String pluginFilePath;

    private LogReplicationConfig logReplicationConfig;

    private LogReplicationServer logReplicationServer;

    private boolean shouldRun = true;

    private ServerContext serverContext;

    private String localCorfuEndpoint;

    private CorfuRuntime runtime;

    /**
     * Constructor Discovery Service
     *
     * @param serverContext
     * @param clusterManager
     */
    public CorfuReplicationDiscoveryService(ServerContext serverContext, CorfuReplicationClusterManagerAdapter clusterManager,
                                            CompletableFuture<LogReplicationContext> discoveryCallback) {
        this.clusterManager = clusterManager;
        this.clusterManager.setCorfuReplicationDiscoveryService(this);
        this.nodeId = serverContext.getNodeId();
        this.serverContext = serverContext;
        this.localEndpoint = serverContext.getLocalEndpoint();
        this.pluginFilePath = serverContext.getPluginConfigFilePath();
        this.discoveryCallback = discoveryCallback;
    }

    public void run() {
        try {
            startDiscovery();

            while (shouldRun) {
                try {
                    while (true) {
                        DiscoveryServiceEvent event = eventQueue.take();
                        processEvent(event);
                    }
                } catch (Exception e) {
                    log.error("Caught an exception. Stop discovery service.", e);
                    shouldRun = false;
                    stopLogReplication();
                    if (e instanceof InterruptedException) {
                        Thread.interrupted();
                    }
                }
            }
        } catch (LogReplicationDiscoveryServiceException e) {
            log.error("Exceptionally terminate Log Replication Discovery Service", e);
            discoveryCallback.completeExceptionally(e);
        } catch (Exception e) {
            log.error("Unhandled exception caught during log replication service discovery", e);
        } finally {
            if (runtime != null) {
                runtime.shutdown();
            }
        }
    }

    /**
     * On first access start topology discovery.
     *
     * On discovery, process the topology information and fetch log replication configuration
     * (streams to replicate) required by an active and standby site before starting
     * log replication.
     */
    private void startDiscovery() throws LogReplicationDiscoveryServiceException {

        try {
            topologyDescriptor = new TopologyDescriptor(clusterManager.fetchTopology());

            // Health check - confirm this node belongs to a cluster in the topology
            if (clusterPresentInTopology(topologyDescriptor, localEndpoint)) {

                log.info("Node[{}] belongs to cluster, descriptor={}", localEndpoint,
                        localClusterDescriptor);

                LogReplicationContext context = buildLogReplicationContext();

                // Unblock server initialization retrieving context: topology + configuration
                discoveryCallback.complete(context);

                registerToLogReplicationLock();
            } else {
                // If a cluster descriptor is not found, this node does not belong to any topology... raise an exception
                String message = String.format("Node[%s] does not belong to any Cluster provided by the discovery service, topology=%s",
                        localEndpoint, topologyDescriptor);
                log.warn(message);
                throw new LogReplicationDiscoveryServiceException(message);
            }
        } catch (Exception e) {
            String message = "Caught exception while fetching topology. Log Replication cannot start.";
            log.error(message, e);
            throw new LogReplicationDiscoveryServiceException(message);
        }
    }

    private LogReplicationContext buildLogReplicationContext() {
        // Through LogReplicationConfigAdapter retrieve system-specific configurations (including streams to replicate)
        logReplicationConfig = getLogReplicationConfiguration(getCorfuRuntime());

        logReplicationMetadataManager = new LogReplicationMetadataManager(getCorfuRuntime(),
                topologyDescriptor.getTopologyConfigId(), localClusterDescriptor.getClusterId());

        logReplicationServer = new LogReplicationServer(serverContext, logReplicationConfig, logReplicationMetadataManager,
                localCorfuEndpoint);
        logReplicationServer.setActive(localClusterDescriptor.getRole().equals(ClusterRole.ACTIVE));

        return new LogReplicationContext(logReplicationConfig, topologyDescriptor, logReplicationServer, localCorfuEndpoint);
    }

    private CorfuRuntime getCorfuRuntime() {
        if (runtime == null) {
            localCorfuEndpoint = getCorfuEndpoint(localEndpoint, localClusterDescriptor.getCorfuPort());
            log.debug("Connecting to local Corfu {}", localCorfuEndpoint);
            runtime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                    .parseConfigurationString(localCorfuEndpoint).connect();
        }

        return runtime;
    }

    /**
     * Verify current node belongs to a cluster in the topology.
     */
    private boolean clusterPresentInTopology(TopologyDescriptor topology, String localEndpoint) {
        localClusterDescriptor = topology.getClusterDescriptor(localEndpoint);
        if (localClusterDescriptor != null) {
            localNodeDescriptor = localClusterDescriptor.getNode(localEndpoint);
            return true;
        }

        return false;
    }

    /**
     * Retrieve local Corfu Endpoint
     */
    private String getCorfuEndpoint(String localEndpoint, int corfuPort) {
        return NodeLocator.parseString(localEndpoint).getHost() + ":" + corfuPort;
    }

    /**
     * Retrieve Log Replication Configuration.
     *
     * This configuration represents all common parameters for the log replication, regardless of
     * a cluster's role.
     */
    private LogReplicationConfig getLogReplicationConfiguration(CorfuRuntime runtime) {

        LogReplicationStreamNameTableManager replicationStreamNameTableManager =
                new LogReplicationStreamNameTableManager(runtime, pluginFilePath);

        Set<String> streamsToReplicate = replicationStreamNameTableManager.getStreamsToReplicate();

        // TODO pankti: Check if version does not match.  If if does not, create an event for site discovery to
        //  do a snapshot sync.
        boolean upgraded = replicationStreamNameTableManager
                .isUpgraded();

        if (upgraded) {
            input(new DiscoveryServiceEvent(DiscoveryServiceEvent.DiscoveryServiceEventType.UPGRADE));
        }

        return new LogReplicationConfig(streamsToReplicate);
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
                    LockClient lock = new LockClient(nodeId, getCorfuRuntime());
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
        if (!localNodeDescriptor.isLeader()) {
            log.warn("Current node {} is not the lead node, log replication cannot be started.", localEndpoint);
            return;
        }

        boolean activeCluster = localNodeDescriptor.getRoleType() == ClusterRole.ACTIVE;

        updateTopologyConfigId(activeCluster);

        if (activeCluster) {
            log.info("Start as Source (sender/replicator) on node {}.", localNodeDescriptor);
            replicationManager = new CorfuReplicationManager(topologyDescriptor, logReplicationConfig,
                    localNodeDescriptor, logReplicationMetadataManager, pluginFilePath, getCorfuRuntime());
            replicationManager.start();
        } else if (localNodeDescriptor.getRoleType() == ClusterRole.STANDBY) {
            // Standby Site : the LogReplicationServer (server handler) will initiate the LogReplicationSinkManager
            log.info("Start as Sink (receiver) on node {} ", localNodeDescriptor);
        } else {
            log.error("Log Replication not started on this cluster. Leader node {} belongs to cluster with {} role.",
                    localEndpoint, localNodeDescriptor.getRoleType());
        }
    }

    private void updateTopologyConfigId(boolean active) {
        // TODO (Xiaoqin Ma): can you please add some info on why this is needed.
        logReplicationServer.getSinkManager()
                .updateSiteConfigID(active, topologyDescriptor.getTopologyConfigId());

        log.debug("Persist new topologyConfigId {}, status={}", topologyDescriptor.getTopologyConfigId(),
                localNodeDescriptor.getRoleType());
    }

    /**
     * Stop ongoing Log Replication
     */
    private void stopLogReplication() {
        if (localNodeDescriptor.isLeader() && localNodeDescriptor.getRoleType() == ClusterRole.ACTIVE) {
            replicationManager.stop();
        }
    }

    /**
     * Process lock acquisition event
     */
    public void processLockAcquire() {
        log.info("Process lock acquire event");

        logReplicationServer.setLeadership(true);

        if (!localNodeDescriptor.isLeader()) {
            // leader transition from false to true, start log replication.
            localNodeDescriptor.setLeader(true);
            startLogReplication();
        }
    }

    /**
     * Process lock release event
     *
     * Set leadership metadata and stop log replication in the event of leadership loss
     */
    public void processLockRelease() {
        logReplicationServer.setLeadership(false);

        if (localNodeDescriptor.isLeader()) {
            stopLogReplication();
            localNodeDescriptor.setLeader(false);
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
            log.debug("Stale Topology Change Notification, current={}, received={}", topologyDescriptor.getTopologyConfigId(), event.getTopologyConfig());
            return;
        }

        TopologyDescriptor newConfig = new TopologyDescriptor(clusterManager.fetchTopology());
        if (newConfig.getTopologyConfigId() == getReplicationManager().getTopologyDescriptor().getTopologyConfigId()) {
            if (localNodeDescriptor.getRoleType() == ClusterRole.STANDBY) {
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

        if (!localNodeDescriptor.isLeader() || localNodeDescriptor.getRoleType() != ClusterRole.ACTIVE) {
            return;
        }

        replicationManager.restart(event.getRemoteSiteInfo());
    }

    /***
     * After an upgrade, the active site should perform a snapshot sync
     */
    private void processUpgrade(DiscoveryServiceEvent event) {
        if (localNodeDescriptor.isLeader() && localNodeDescriptor.getRoleType() == ClusterRole.ACTIVE) {
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

    public synchronized void input(DiscoveryServiceEvent event) {
        eventQueue.add(event);
        notifyAll();
    }

    @Override
    public void updateSiteConfig(LogReplicationClusterInfo.TopologyConfigurationMsg topologyConfig) {
        input(new DiscoveryServiceEvent(DiscoveryServiceEvent.DiscoveryServiceEventType.DISCOVERY_SITE, topologyConfig));
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
