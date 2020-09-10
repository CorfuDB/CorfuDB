package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.LogReplicationServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.CorfuReplicationClusterManagerAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationStreamNameTableManager;
import org.corfudb.infrastructure.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.utils.lock.Lock;
import org.corfudb.utils.lock.LockClient;
import org.corfudb.utils.lock.LockListener;
import org.corfudb.utils.lock.states.HasLeaseState;
import org.corfudb.utils.lock.states.LockState;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents the Log Replication Discovery Service.
 *
 * It manages the following:
 *
 * - Discover topology and determine cluster role: active/standby
 * - Lock acquisition (leader election), node leading the log replication sending and receiving
 * - Log replication configuration (streams to replicate)
 */
@Slf4j
public class CorfuReplicationDiscoveryService implements Runnable, CorfuReplicationDiscoveryServiceAdapter {

    /**
     * Wait interval (in seconds) between consecutive fetch topology attempts to cap exponential back-off.
     */
    private static final int FETCH_THRESHOLD = 300;

    /**
     * Fraction of Lease Duration for Lease Renewal
     */
    private static final int RENEWAL_LEASE_FRACTION = 4;

    /**
     * Fraction of Lease Duration for Lease Monitoring
     */
    private static final int MONITOR_LEASE_FRACTION = 10;

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
     * System exit error code called by the Corfu Runtime systemDownHandler
     */
    private static final int SYSTEM_EXIT_ERROR_CODE = -3;
    /**
     * Used by the active cluster to initiate Log Replication
     */
    @Getter
    private CorfuReplicationManager replicationManager;

    /**
     * Adapter for cluster discovery service
     */
    @Getter
    private CorfuReplicationClusterManagerAdapter clusterManagerAdapter;

    /**
     * Defines the topology, which is discovered through the Cluster Manager
     */
    private TopologyDescriptor topologyDescriptor;

    /**
     * Defines the cluster to which this node belongs to.
     */
    private ClusterDescriptor localClusterDescriptor;

    /**
     * Current node's endpoint
     */
    private final String localEndpoint;

    /**
     * Current node information
     */
    private NodeDescriptor localNodeDescriptor;

    /**
     * Unique node identifier
     */
    // Note: not to be confused with NodeDescriptor's NodeId, which is a unique
    // identifier for the node as reported by the Cluster/Topology Manager
    // This node Id is internal to Corfu Log Replication and used for the lock acquisition
    private final UUID logReplicationNodeId;

    /**
     * A queue of Discovery Service events
     */
    private final LinkedBlockingQueue<DiscoveryServiceEvent> eventQueue = new LinkedBlockingQueue<>();

    /**
     * Callback to Log Replication Server upon topology discovery
     */
    private CompletableFuture<CorfuInterClusterReplicationServerNode> serverCallback;

    private CorfuInterClusterReplicationServerNode interClusterReplicationService;

    private ServerContext serverContext;

    private String localCorfuEndpoint;

    private CorfuRuntime runtime;

    private LogReplicationContext replicationContext;

    private boolean shouldRun = true;

    private volatile AtomicBoolean isLeader;

    private LogReplicationServer logReplicationServerHandler;

    private LockClient lockClient;

    /**
     * Indicates the server has been started. A server is started once it is determined
     * that this node belongs to a cluster in the topology provided by ClusterManager.
     */
    private boolean serverStarted = false;

    /**
     * Constructor Discovery Service
     *
     * @param serverContext current server's context
     * @param clusterManagerAdapter adapter to communicate to external Cluster Manager
     * @param serverCallback callback to Log Replication Server upon discovery
     *
     */
    public CorfuReplicationDiscoveryService(@Nonnull ServerContext serverContext,
                                            @Nonnull CorfuReplicationClusterManagerAdapter clusterManagerAdapter,
                                            @Nonnull CompletableFuture<CorfuInterClusterReplicationServerNode> serverCallback) {
        this.clusterManagerAdapter = clusterManagerAdapter;
        this.logReplicationNodeId = serverContext.getNodeId();
        this.serverContext = serverContext;
        this.localEndpoint = serverContext.getLocalEndpoint();
        this.serverCallback = serverCallback;
        this.isLeader = new AtomicBoolean();
    }

    public void run() {
        try {
            startDiscovery();

            while (shouldRun) {
                try {
                    DiscoveryServiceEvent event = eventQueue.take();
                    processEvent(event);
                } catch (Exception e) {
                    // TODO: We should take care of which exceptions really end up being
                    //  caught at this level, or we could be stopping LR completely on
                    //  any exception.
                    log.error("Caught an exception. Stop discovery service.", e);
                    shouldRun = false;
                    stopLogReplication();
                    if (e instanceof InterruptedException) {
                        Thread.interrupted();
                    }
                }
            }
        } catch (Exception e) {
            log.error("Unhandled exception caught during log replication service discovery.", e);
        } finally {
            if (runtime != null) {
                runtime.shutdown();
            }

            interClusterReplicationService.close();
        }
    }

    /**
     * Process discovery event
     */
    public void processEvent(DiscoveryServiceEvent event) {
        switch (event.type) {
            case ACQUIRE_LOCK:
                processLockAcquire();
                break;

            case RELEASE_LOCK:
                processLockRelease();
                break;

            case DISCOVERED_TOPOLOGY:
                processTopologyChangeNotification(event);
                break;

            case UPGRADE:
                processUpgrade(event);
                break;

            default:
                log.error("Invalid event type {}", event.type);
                break;
        }
    }

    /**
     * On first access start topology discovery.
     *
     * On discovery, process the topology information and fetch log replication configuration
     * (streams to replicate) required by an active and standby site before starting
     * log replication.
     */
    private void startDiscovery() {
        log.info("Start Log Replication Discovery Service");
        connectToClusterManager();
        fetchTopologyFromClusterManager();
        processDiscoveredTopology(topologyDescriptor, true);
    }

    /**
     * Connect to Cluster Manager
     */
    private void connectToClusterManager() {
        // The ClusterManager orchestrates the Log Replication Service. If it is not available,
        // topology cannot be discovered and therefore LR cannot start, for this reason connection
        // should be attempted indefinitely.
        try {
            clusterManagerAdapter.register(this);

            IRetry.build(IntervalRetry.class, () -> {
                try {
                    log.info("Connecting to Cluster Manager {}", clusterManagerAdapter.getClass().getSimpleName());
                    clusterManagerAdapter.start();
                } catch (Exception e) {
                    log.error("Error while attempting to connect to ClusterManager.", e);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to connect to ClusterManager.", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Bootstrap the Log Replication Service, which includes:
     *
     * - Building Log Replication Context (LR context shared across receiving and sending components)
     * - Start Log Replication Server (receiver component)
     */
    private void bootstrapLogReplicationService() {
        // Through LogReplicationConfigAdapter retrieve system-specific configurations (including streams to replicate)
        LogReplicationConfig logReplicationConfig = getLogReplicationConfiguration(getCorfuRuntime());

        logReplicationMetadataManager = new LogReplicationMetadataManager(getCorfuRuntime(),
                topologyDescriptor.getTopologyConfigId(), localClusterDescriptor.getClusterId());

        logReplicationServerHandler = new LogReplicationServer(serverContext, logReplicationConfig,
                logReplicationMetadataManager, localCorfuEndpoint, topologyDescriptor.getTopologyConfigId());
        logReplicationServerHandler.setActive(localClusterDescriptor.getRole().equals(ClusterRole.ACTIVE));

        interClusterReplicationService = new CorfuInterClusterReplicationServerNode(serverContext,
                logReplicationServerHandler, logReplicationConfig);

        // Pass server's channel context through the Log Replication Context, for shared objects between the server
        // and the client channel (specific requirements of the transport implementation)
        replicationContext = new LogReplicationContext(logReplicationConfig, topologyDescriptor,
                localCorfuEndpoint, interClusterReplicationService.getRouter().getServerAdapter().getChannelContext());

        // Unblock server initialization & register to Log Replication Lock, to attempt lock / leadership acquisition
        serverCallback.complete(interClusterReplicationService);

        serverStarted = true;
    }

    /**
     * Retrieve a Corfu Runtime to connect to the local Corfu Datastore.
     */
    private CorfuRuntime getCorfuRuntime() {
        // Avoid multiple runtime's
        if (runtime == null) {
            log.debug("Connecting to local Corfu {}", localCorfuEndpoint);
            runtime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder()
                    .trustStore((String) serverContext.getServerConfig().get("--truststore"))
                    .tsPasswordFile((String) serverContext.getServerConfig().get("--truststore-password-file"))
                    .keyStore((String) serverContext.getServerConfig().get("--keystore"))
                    .ksPasswordFile((String) serverContext.getServerConfig().get("--keystore-password-file"))
                    .tlsEnabled((Boolean) serverContext.getServerConfig().get("--enable-tls"))
                    .systemDownHandler(() -> System.exit(SYSTEM_EXIT_ERROR_CODE))
                    .build())
                    .parseConfigurationString(localCorfuEndpoint).connect();
        }

        return runtime;
    }

    /**
     * Verify current node belongs to a cluster in the topology.
     *
     * @param topology discovered topology
     * @param update indicates if the discovered topology should immediately be reflected as current (cached)
     */
    private boolean clusterPresentInTopology(TopologyDescriptor topology, boolean update) {
        ClusterDescriptor tmpClusterDescriptor = topology.getClusterDescriptor(localEndpoint);
        NodeDescriptor tmpNodeDescriptor = null;

        if (tmpClusterDescriptor != null) {
            tmpNodeDescriptor = tmpClusterDescriptor.getNode(localEndpoint);

            if (update) {
                localClusterDescriptor = tmpClusterDescriptor;
                localNodeDescriptor = tmpNodeDescriptor;
                localCorfuEndpoint = getCorfuEndpoint(getLocalHost(), localClusterDescriptor.getCorfuPort());
            }
        }

        return tmpClusterDescriptor != null && tmpNodeDescriptor != null;
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
                new LogReplicationStreamNameTableManager(runtime, serverContext.getPluginConfigFilePath());

        Set<String> streamsToReplicate = replicationStreamNameTableManager.getStreamsToReplicate();

        // TODO pankti: Check if version does not match. If it does not, create an event for site discovery to
        //  do a snapshot sync.
        boolean upgraded = replicationStreamNameTableManager
                .isUpgraded();

        if (upgraded) {
            input(new DiscoveryServiceEvent(DiscoveryServiceEvent.DiscoveryServiceEventType.UPGRADE));
        }

        return new LogReplicationConfig(streamsToReplicate, serverContext.getLogReplicationMaxNumMsgPerBatch(), serverContext.getLogReplicationMaxDataMessageSize());
    }

    /**
     * Register interest on Log Replication Lock.
     *
     * The node that acquires the lock will drive/lead log replication.
     */
    private void registerToLogReplicationLock() {
        try {

            Lock.setLeaseDuration(serverContext.getLockLeaseDuration());
            LockClient.setDurationBetweenLockMonitorRuns(serverContext.getLockLeaseDuration()/MONITOR_LEASE_FRACTION);
            LockState.setDurationBetweenLeaseRenewals(serverContext.getLockLeaseDuration()/RENEWAL_LEASE_FRACTION);
            HasLeaseState.setDurationBetweenLeaseChecks(serverContext.getLockLeaseDuration()/MONITOR_LEASE_FRACTION);

            IRetry.build(IntervalRetry.class, () -> {
                try {
                    lockClient = new LockClient(logReplicationNodeId, getCorfuRuntime());
                    // Callback on lock acquisition or revoke
                    LockListener logReplicationLockListener = new LogReplicationLockListener(this);
                    // Register Interest on the shared Log Replication Lock
                    lockClient.registerInterest(LOCK_GROUP, LOCK_NAME, logReplicationLockListener);
                } catch (Exception e) {
                    log.error("Error while attempting to register interest on log replication lock {}:{}", LOCK_GROUP, LOCK_NAME, e);
                    throw new RetryNeededException();
                }

                log.debug("Registered to lock, client msb={}, lsb={}", logReplicationNodeId.getMostSignificantBits(),
                        logReplicationNodeId.getLeastSignificantBits());
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to register interest on log replication lock.", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * This method is only called on the leader node and it triggers the start of log replication
     *
     * Depending on the role of the cluster to which this leader node belongs to, it will start
     * as source (sender/producer) or sink (receiver).
     */
    private void onLeadershipAcquire() {
        switch (localClusterDescriptor.getRole()) {
            case ACTIVE:
                log.info("Start as Source (sender/replicator)");
                if (replicationManager == null) {
                    replicationManager = new CorfuReplicationManager(replicationContext,
                            localNodeDescriptor, logReplicationMetadataManager, serverContext.getPluginConfigFilePath(),
                            getCorfuRuntime());
                }
                replicationManager.setTopology(topologyDescriptor);
                replicationManager.start();
                break;
            case STANDBY:
                // Standby Site : the LogReplicationServer (server handler) will initiate the LogReplicationSinkManager
                log.info("Start as Sink (receiver)");
                interClusterReplicationService.getLogReplicationServer().getSinkManager().reset();
                interClusterReplicationService.getLogReplicationServer().setLeadership(true);
                break;
            default:
                log.error("Log Replication not started on this cluster. Leader node {} belongs to cluster with {} role.",
                            localEndpoint, localClusterDescriptor.getRole());
                break;
        }
    }

    /**
     * Fetch current topology from Cluster Manager
     */
    private void fetchTopologyFromClusterManager() {
        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try {
                    log.info("Fetching topology from Cluster Manager...");
                    TopologyConfigurationMsg topologyMessage = clusterManagerAdapter.queryTopologyConfig(false);
                    topologyDescriptor = new TopologyDescriptor(topologyMessage);
                } catch (Exception e) {
                    log.error("Caught exception while fetching topology. Retry.", e);
                    throw new RetryNeededException();
                }

                return null;
            }).setOptions(x -> x.setMaxRetryThreshold(Duration.ofSeconds(FETCH_THRESHOLD))).run();
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        } catch (RetryExhaustedException ree) {
            // Retries exhausted. Return
            log.warn("Failed to retrieve updated topology from Cluster Manager.");
        }
    }

    /**
     * Stop Log Replication
     */
    private void stopLogReplication() {
        if (localClusterDescriptor != null && localClusterDescriptor.getRole() == ClusterRole.ACTIVE && isLeader.get()) {
            log.info("Stopping log replication.");
            replicationManager.stop();
        }
    }

    /**
     * Process lock acquisition event
     */
    public void processLockAcquire() {
        log.debug("Lock acquired");
        isLeader.set(true);
        onLeadershipAcquire();
    }

    /**
     * Update Topology Config Id on MetadataManager (persisted metadata table)
     * and push down to Sink Manager so messages are filtered on the most
     * up to date topologyConfigId
     */
    private void updateTopologyConfigId(long configId) {
        this.logReplicationMetadataManager.setupTopologyConfigId(configId);
        this.interClusterReplicationService.getLogReplicationServer()
                .getSinkManager().updateTopologyConfigId(configId);
    }

    /**
     * Process lock release event
     *
     * Set leadership metadata and stop log replication in the event of leadership loss
     */
    public void processLockRelease() {
        log.debug("Lock released");
        isLeader.set(false);
        stopLogReplication();
        // Signal Log Replication Server/Sink to stop receiving messages, leadership loss
        interClusterReplicationService.getLogReplicationServer().setLeadership(false);
    }

    /**
     * Process Topology Config Change:
     *   - Higher config id
     *   - Potential cluster role change
     *
     * Cluster change from active to standby is a two step process, we first confirm that
     * we are ready to do the cluster role change, so by the time we receive cluster change
     * notification, nothing needs to be done, other than stop.
     * @param newTopology new discovered topology
     */
    public void onClusterRoleChange(TopologyDescriptor newTopology) {

        log.debug("OnClusterRoleChange, topology={}", newTopology);

        // Stop ongoing replication, stopLogReplication() checks leadership and active
        // We do not update topology until we successfully stop log replication
        if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE) {
            stopLogReplication();
        }

        // Update topology, cluster, and node configs
        updateLocalTopology(newTopology);

        // Update topology config id in metadata manager
        logReplicationMetadataManager.setupTopologyConfigId(topologyDescriptor.getTopologyConfigId());

        // Reset the Replication Status on Active and Standby
        logReplicationMetadataManager.resetReplicationStatus();

        log.debug("Persist new topologyConfigId {}, cluster id={}, role={}", topologyDescriptor.getTopologyConfigId(),
                localClusterDescriptor.getClusterId(), localClusterDescriptor.getRole());

        // Update replication manager
        updateReplicationManagerTopology(newTopology);

        // Update sink manager
        interClusterReplicationService.getLogReplicationServer().getSinkManager()
                .updateTopologyConfigId(topologyDescriptor.getTopologyConfigId());
        interClusterReplicationService.getLogReplicationServer().getSinkManager().reset();

        // Update replication server, in case there is a role change
        logReplicationServerHandler.setActive(localClusterDescriptor.getRole().equals(ClusterRole.ACTIVE));

        // On Topology Config Change, only if this node is the leader take action
        if (isLeader.get()) {
            onLeadershipAcquire();
        }
    }

    /**
     * Process a topology change as provided by the Cluster Manager
     *
     * Note: We are assuming that topology configId change implies a role change.
     *       The number of standby clusters change would not bump config id.
     *
     * @param event discovery event
     */
    public void processTopologyChangeNotification(DiscoveryServiceEvent event) {
        // Skip stale topology notification
        if (event.getTopologyConfig().getTopologyConfigID() < topologyDescriptor.getTopologyConfigId()) {
            log.debug("Stale Topology Change Notification, current={}, received={}",
                    topologyDescriptor.getTopologyConfigId(), event.getTopologyConfig());
            return;
        }

        log.debug("Received topology change, topology={}", event.getTopologyConfig());

        TopologyDescriptor discoveredTopology = new TopologyDescriptor(event.getTopologyConfig());
        boolean isValid = processDiscoveredTopology(discoveredTopology, localClusterDescriptor == null);

        if (isValid) {
            if (clusterRoleChanged(discoveredTopology)) {
                onClusterRoleChange(discoveredTopology);
            } else {
                onStandbyClusterAddRemove(discoveredTopology);
            }
        } else {
            // Stop Log Replication in case this node was previously ACTIVE but no longer belongs to the Topology
            stopLogReplication();
        }
    }

    /**
     * Determine if there was a cluster change between former topology and newly discovered
     *
     * @return true, cluster role changed
     *         false, otherwise
     */
    private boolean clusterRoleChanged(TopologyDescriptor discoveredTopology) {
        if (localClusterDescriptor != null) {
            return localClusterDescriptor.getRole() != discoveredTopology.getClusterDescriptor(localEndpoint).getRole();
        }

        return false;
    }

    /**
     * Process a topology change where a standby cluster has been added or removed from the topology.
     *
     * @param discoveredTopology new discovered topology
     */
    private void onStandbyClusterAddRemove(TopologyDescriptor discoveredTopology) {
        log.debug("Standby Cluster has been added or removed from topology={}", discoveredTopology);

        // We only need to process new standby's if your role is of an ACTIVE cluster
        if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE) {
            if (replicationManager != null && isLeader.get()) {
                replicationManager.processStandbyChange(discoveredTopology);
            }
        }

        updateLocalTopology(discoveredTopology);
        updateReplicationManagerTopology(discoveredTopology);
        updateTopologyConfigId(topologyDescriptor.getTopologyConfigId());
        log.debug("Persist new topologyConfigId {}, cluster id={}, role={}", topologyDescriptor.getTopologyConfigId(),
                localClusterDescriptor.getClusterId(), localClusterDescriptor.getRole());
    }

    /**
     * Process the discovered topology to determine if it is a valid topology view.
     *
     * @param topology cluster manager provided topology
     * @param update indicates if the discovered topology should immediately be reflected as current (cached)
     *
     * @return true, valid topology
     *         false, otherwise
     */
    private boolean processDiscoveredTopology(TopologyDescriptor topology, boolean update) {
        // Health check - confirm this node belongs to a cluster in the topology
        if (topology != null && clusterPresentInTopology(topology, update)) {
            log.info("Node[{}] belongs to cluster, descriptor={}, topology={}", localEndpoint, localClusterDescriptor, topology);
            if (!serverStarted) {
                bootstrapLogReplicationService();
                registerToLogReplicationLock();
            }
            return true;
        }

        // If a cluster descriptor is not found, this node does not belong to any cluster in the topology
        // wait for updates to the topology config to start, if this cluster ever becomes part of the topology
        log.warn("Node[{}] does not belong to any cluster provided by the discovery service, topology={}", localEndpoint,
                topology);
        return false;
    }

    private void updateLocalTopology(TopologyDescriptor newConfig) {
        // Update local topology descriptor
        topologyDescriptor = newConfig;

        // Update local cluster descriptor
        localClusterDescriptor = topologyDescriptor.getClusterDescriptor(localEndpoint);

        // Update local node descriptor
        localNodeDescriptor = localClusterDescriptor.getNode(localEndpoint);
    }

    private void updateReplicationManagerTopology(TopologyDescriptor newConfig) {
        if (replicationManager != null) {
            replicationManager.updateRuntimeConfigId(newConfig);
        }
    }

    /***
     * After an upgrade, the active site should perform a snapshot sync
     */
    private void processUpgrade(DiscoveryServiceEvent event) {
        if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE) {
            // TODO pankti: is this correct?
            replicationManager.restart(event.getRemoteSiteInfo());
        }
    }

    public synchronized void input(DiscoveryServiceEvent event) {
        eventQueue.add(event);
        notifyAll();
    }

    @Override
    public void updateTopology(LogReplicationClusterInfo.TopologyConfigurationMsg topologyConfig) {
        input(new DiscoveryServiceEvent(DiscoveryServiceEventType.DISCOVERED_TOPOLOGY, topologyConfig));
    }

    /**
     * Active Cluster - Read the shared metadata table to find the status of any ongoing snapshot or log entry sync
     * and return a completion percentage.
     *
     * Standby Cluster - Read the shared metadata table and find if data is consistent(returns false if
     * snapshot sync is in the apply phase)
     */
    @Override
    public Map<String, LogReplicationMetadata.ReplicationStatusVal> queryReplicationStatus() {
        if (ClusterRole.ACTIVE == localClusterDescriptor.getRole()) {
            Map<String, LogReplicationMetadata.ReplicationStatusVal> mapReplicationStatus = logReplicationMetadataManager.getReplicationRemainingEntries();
            Map<String, LogReplicationMetadata.ReplicationStatusVal> mapToSend = new HashMap<>(mapReplicationStatus);
            // If map contains local cluster, remove (as it might have been added by the SinkManager) but this node
            // has an active role.
            if (mapToSend.containsKey(localClusterDescriptor.getClusterId())) {
                log.warn("Remove localClusterDescriptor {} from replicationStatusMap", localClusterDescriptor.getClusterId());
                mapToSend.remove(localClusterDescriptor.getClusterId());
            }
            return mapToSend;
        } else if (ClusterRole.STANDBY == localClusterDescriptor.getRole()) {
            return logReplicationMetadataManager.getDataConsistentOnStandby();
        }
        log.error("Received Replication Status Query in Incorrect Role {}.", localClusterDescriptor.getRole());
        return null;
    }

    public void shutdown() {
        if (replicationManager != null) {
            replicationManager.stop();
        }

        if (clusterManagerAdapter != null) {
            clusterManagerAdapter.shutdown();
        }

        if (lockClient != null) {
            lockClient.shutdown();
        }
    }

    /**
     * Return host for local node
     *
     * @return current node's IP
     */
    private String getLocalHost() {
        return NodeLocator.parseString(serverContext.getLocalEndpoint()).getHost();
    }
}
