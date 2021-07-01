package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Tag;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.LogReplicationServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.CorfuReplicationClusterManagerAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEventKey;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationStreamNameTableManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class represents the Log Replication Discovery Service.
 * <p>
 * It manages the following:
 * <p>
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
    private final CorfuReplicationClusterManagerAdapter clusterManagerAdapter;

    /**
     * Defines the topology, which is discovered through the Cluster Manager
     */
    private TopologyDescriptor topologyDescriptor;

    /**
     * Defines the cluster to which this node belongs to.
     */
    @Getter
    private ClusterDescriptor localClusterDescriptor;

    /**
     * Current node's endpoint
     */
    private final String localEndpoint;

    /**
     * Current node's id
     */
    private String localNodeId;

    /**
     * Current node information
     */
    @Getter
    private NodeDescriptor localNodeDescriptor;

    /**
     * Unique node identifier of lock
     */
    // Note: not to be confused with NodeDescriptor's NodeId, which is a unique
    // identifier for the node as reported by the Cluster/Topology Manager
    // This node Id is internal to Corfu Log Replication and used for the lock acquisition
    @Getter
    private final UUID logReplicationLockId;

    /**
     * A queue of Discovery Service events
     */
    private final LinkedBlockingQueue<DiscoveryServiceEvent> eventQueue = new LinkedBlockingQueue<>();


    private Optional<LongTaskTimer.Sample> lockAcquireSample = Optional.empty();

    private final Map<ClusterRole, AtomicLong> lockAcquisitionsByRole = new HashMap<>();
    /**
     * Callback to Log Replication Server upon topology discovery
     */
    private final CompletableFuture<CorfuInterClusterReplicationServerNode> serverCallback;

    private CorfuInterClusterReplicationServerNode interClusterReplicationService;

    private final ServerContext serverContext;

    private String localCorfuEndpoint;

    private CorfuRuntime runtime;

    private LogReplicationContext replicationContext;

    private boolean shouldRun = true;

    @Getter
    private final AtomicBoolean isLeader;

    private LogReplicationServer logReplicationServerHandler;

    private LockClient lockClient;

    /**
     * Indicates the server has been started. A server is started once it is determined
     * that this node belongs to a cluster in the topology provided by ClusterManager.
     */
    private boolean serverStarted = false;

    /**
     * Indicates the replication status has been set as NOT_STARTED.
     * It should be reset if its role changes to Standby.
     */
    private boolean statusFlag = false;

    /**
     * This is the listener to the replication event table shared by the nodes in the cluster.
     * When a non-leader node is called to do the enforcedSnapshotSync, it will write the event to
     * the shared event-table and the leader node will be notified to do the work.
     */
    private LogReplicationEventListener logReplicationEventListener;

    /**
     * Constructor Discovery Service
     *
     * @param serverContext         current server's context
     * @param clusterManagerAdapter adapter to communicate to external Cluster Manager
     * @param serverCallback        callback to Log Replication Server upon discovery
     */
    public CorfuReplicationDiscoveryService(@Nonnull ServerContext serverContext,
                                            @Nonnull CorfuReplicationClusterManagerAdapter clusterManagerAdapter,
                                            @Nonnull CompletableFuture<CorfuInterClusterReplicationServerNode> serverCallback) {
        this.clusterManagerAdapter = clusterManagerAdapter;
        this.logReplicationLockId = serverContext.getNodeId();
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
    public synchronized void processEvent(DiscoveryServiceEvent event) {
        switch (event.getType()) {
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

            case ENFORCE_SNAPSHOT_SYNC:
                processEnforceSnapshotSync(event);
                break;

            default:
                log.error("Invalid event type {}", event.getType());
                break;
        }
    }

    /**
     * On first access start topology discovery.
     * <p>
     * On discovery, process the topology information and fetch log replication configuration
     * (streams to replicate) required by an active and standby site before starting
     * log replication.
     */
    private void startDiscovery() {
        log.info("Start Log Replication Discovery Service");
        setupLocalNodeId();
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
     * <p>
     * - Building Log Replication Context (LR context shared across receiving and sending components)
     * - Start Log Replication Server (receiver component)
     */
    private void bootstrapLogReplicationService() {
        // Through LogReplicationConfigAdapter retrieve system-specific configurations
        // such as streams to replicate and version
        LogReplicationConfig logReplicationConfig = getLogReplicationConfiguration(getCorfuRuntime());

        logReplicationMetadataManager = new LogReplicationMetadataManager(getCorfuRuntime(),
            topologyDescriptor.getTopologyConfigId(), localClusterDescriptor.getClusterId());

        logReplicationServerHandler = new LogReplicationServer(serverContext, logReplicationConfig,
            logReplicationMetadataManager, localCorfuEndpoint, topologyDescriptor.getTopologyConfigId(), localNodeId);
        logReplicationServerHandler.setActive(localClusterDescriptor.getRole().equals(ClusterRole.ACTIVE));

        interClusterReplicationService = new CorfuInterClusterReplicationServerNode(serverContext,
            logReplicationServerHandler, logReplicationConfig);

        // Pass server's channel context through the Log Replication Context, for shared objects between the server
        // and the client channel (specific requirements of the transport implementation)
        replicationContext = new LogReplicationContext(logReplicationConfig, topologyDescriptor,
            localCorfuEndpoint, interClusterReplicationService.getRouter().getServerAdapter().getChannelContext());

        // Unblock server initialization & register to Log Replication Lock, to attempt lock / leadership acquisition
        serverCallback.complete(interClusterReplicationService);

        logReplicationEventListener = new LogReplicationEventListener(this);
        logReplicationEventListener.start();
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
                    .trustStore(serverContext.getConfiguration().getTruststore())
                    .tsPasswordFile(serverContext.getConfiguration().getTruststorePasswordFile())
                    .keyStore(serverContext.getConfiguration().getKeystore())
                    .ksPasswordFile(serverContext.getConfiguration().getKeystorePasswordFile())
                    .tlsEnabled(serverContext.getConfiguration().isTlsEnabled())
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
     * @param update   indicates if the discovered topology should immediately be reflected as current (cached)
     */
    private boolean clusterPresentInTopology(TopologyDescriptor topology, boolean update) {
        ClusterDescriptor tmpClusterDescriptor = topology.getClusterDescriptor(localNodeId);
        NodeDescriptor tmpNodeDescriptor = null;

        if (tmpClusterDescriptor != null) {
            tmpNodeDescriptor = tmpClusterDescriptor.getNode(localNodeId);

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
     * <p>
     * This configuration represents all common parameters for the log replication, regardless of
     * a cluster's role.
     */
    private LogReplicationConfig getLogReplicationConfiguration(CorfuRuntime runtime) {

        try {
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
        } catch (Throwable t) {
            log.error("Exception when fetching the Replication Config", t);
            throw t;
        }
    }

    /**
     * Register interest on Log Replication Lock.
     * <p>
     * The node that acquires the lock will drive/lead log replication.
     */
    private void registerToLogReplicationLock() {
        try {

            Lock.setLeaseDuration(serverContext.getLockLeaseDuration());
            LockClient.setDurationBetweenLockMonitorRuns(serverContext.getLockLeaseDuration() / MONITOR_LEASE_FRACTION);
            LockState.setDurationBetweenLeaseRenewals(serverContext.getLockLeaseDuration() / RENEWAL_LEASE_FRACTION);
            HasLeaseState.setDurationBetweenLeaseChecks(serverContext.getLockLeaseDuration() / MONITOR_LEASE_FRACTION);

            IRetry.build(IntervalRetry.class, () -> {
                try {
                    lockClient = new LockClient(logReplicationLockId, getCorfuRuntime());
                    // Callback on lock acquisition or revoke
                    LockListener logReplicationLockListener = new LogReplicationLockListener(this);
                    // Register Interest on the shared Log Replication Lock
                    lockClient.registerInterest(LOCK_GROUP, LOCK_NAME, logReplicationLockListener);
                } catch (Exception e) {
                    log.error("Error while attempting to register interest on log replication lock {}:{}", LOCK_GROUP, LOCK_NAME, e);
                    throw new RetryNeededException();
                }

                log.debug("Registered to lock, client msb={}, lsb={}", logReplicationLockId.getMostSignificantBits(),
                        logReplicationLockId.getLeastSignificantBits());
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to register interest on log replication lock.", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * This method is only called on the leader node and it triggers the start of log replication
     * <p>
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
                updateReplicationStatus();
                lockAcquireSample = recordLockAcquire(localClusterDescriptor.getRole());
                processCountOnLockAcquire(localClusterDescriptor.getRole());
                break;
            case STANDBY:
                // Standby Site : the LogReplicationServer (server handler) will initiate the LogReplicationSinkManager
                log.info("Start as Sink (receiver)");
                interClusterReplicationService.getLogReplicationServer().getSinkManager().reset();
                interClusterReplicationService.getLogReplicationServer().setLeadership(true);
                statusFlag = false;
                lockAcquireSample = recordLockAcquire(localClusterDescriptor.getRole());
                processCountOnLockAcquire(localClusterDescriptor.getRole());
                break;
            default:
                log.error("Log Replication not started on this cluster. Leader node {} id {} belongs to cluster with {} role.",
                        localEndpoint, localNodeId, localClusterDescriptor.getRole());
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
     * <p>
     * Set leadership metadata and stop log replication in the event of leadership loss
     */
    public void processLockRelease() {
        log.debug("Lock released");
        // Unset isLeader flag after stopping log replication
        stopLogReplication();
        isLeader.set(false);
        // Signal Log Replication Server/Sink to stop receiving messages, leadership loss
        interClusterReplicationService.getLogReplicationServer().setLeadership(false);
        recordLockRelease();
    }

    /**
     * Process Topology Config Change:
     * - Higher config id
     * - Potential cluster role change
     * <p>
     * Cluster change from active to standby is a two step process, we first confirm that
     * we are ready to do the cluster role change, so by the time we receive cluster change
     * notification, nothing needs to be done, other than stop.
     *
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
        resetReplicationStatusTableWithRetry();

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
     * <p>
     * Note: We are assuming that topology configId change implies a role change.
     * The number of standby clusters change would not bump config id.
     *
     * @param event discovery event
     */
    public void processTopologyChangeNotification(DiscoveryServiceEvent event) {
        // Skip stale topology notification
        if (event.getTopologyConfig().getTopologyConfigID() < topologyDescriptor.getTopologyConfigId()) {
            log.debug("Stale Topology Change Notification, current={}, received={}",
                    topologyDescriptor.getTopologyConfigId(), event.getTopologyConfig().getTopologyConfigID());
            return;
        }

        log.debug("Received topology change, topology={}", event.getTopologyConfig());

        TopologyDescriptor discoveredTopology = new TopologyDescriptor(event.getTopologyConfig());

        boolean isValid;
        try {
            isValid = processDiscoveredTopology(discoveredTopology, localClusterDescriptor == null);
        } catch (Throwable t) {
            log.error("Exception when processing the discovered topology", t);
            stopLogReplication();
            return;
        }

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
     * false, otherwise
     */
    private boolean clusterRoleChanged(TopologyDescriptor discoveredTopology) {
        if (localClusterDescriptor != null) {
            return localClusterDescriptor.getRole() !=
                    discoveredTopology.getClusterDescriptor(localNodeId).getRole();
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
        if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE && replicationManager != null && isLeader.get()) {
            replicationManager.processStandbyChange(discoveredTopology);
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
     * @param update   indicates if the discovered topology should immediately be reflected as current (cached)
     * @return true, valid topology
     * false, otherwise
     */
    private boolean processDiscoveredTopology(TopologyDescriptor topology, boolean update) {
        // Health check - confirm this node belongs to a cluster in the topology
        if (topology != null && clusterPresentInTopology(topology, update)) {
            log.info("Node[{}/{}] belongs to cluster, descriptor={}, topology={}", localEndpoint, localNodeId, localClusterDescriptor, topology);
            if (!serverStarted) {
                bootstrapLogReplicationService();
                registerToLogReplicationLock();
            }
            return true;
        }

        // If a cluster descriptor is not found, this node does not belong to any cluster in the topology
        // wait for updates to the topology config to start, if this cluster ever becomes part of the topology
        log.warn("Node[{}/{}] does not belong to any cluster provided by the discovery service, topology={}",
                localEndpoint, localNodeId, topology);
        return false;
    }

    private void updateLocalTopology(TopologyDescriptor newConfig) {
        // Update local topology descriptor
        topologyDescriptor = newConfig;

        // Update local cluster descriptor
        localClusterDescriptor = topologyDescriptor.getClusterDescriptor(localNodeId);

        // Update local node descriptor
        localNodeDescriptor = localClusterDescriptor.getNode(localNodeId);
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
            replicationManager.restart(event.getRemoteClusterInfo());
        }
    }

    /**
     * Enforce a snapshot full sync for all standbys if the current node is a leader node
     */
    private void processEnforceSnapshotSync(DiscoveryServiceEvent event) {
        if (replicationManager == null || !isLeader.get()) {
            log.warn("The current node is not the leader, will skip doing the " +
                    "forced snapshot sync with id {}", event.getEventId());
            return;
        }

        replicationManager.enforceSnapshotSync(event);
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
     * <p>
     * Standby Cluster - Read the shared metadata table and find if data is consistent(returns false if
     * snapshot sync is in the apply phase)
     */
    @Override
    public Map<String, LogReplicationMetadata.ReplicationStatusVal> queryReplicationStatus() {
        if (localClusterDescriptor == null) {
            log.warn("Cluster configuration has not been pushed to current LR node.");
            return null;
        } else if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE) {
            Map<String, LogReplicationMetadata.ReplicationStatusVal> mapReplicationStatus = logReplicationMetadataManager.getReplicationRemainingEntries();
            Map<String, LogReplicationMetadata.ReplicationStatusVal> mapToSend = new HashMap<>(mapReplicationStatus);
            // If map contains local cluster, remove (as it might have been added by the SinkManager) but this node
            // has an active role.
            if (mapToSend.containsKey(localClusterDescriptor.getClusterId())) {
                log.warn("Remove localClusterDescriptor {} from replicationStatusMap", localClusterDescriptor.getClusterId());
                mapToSend.remove(localClusterDescriptor.getClusterId());
            }
            return mapToSend;
        } else if (localClusterDescriptor.getRole() == ClusterRole.STANDBY) {
            return logReplicationMetadataManager.getDataConsistentOnStandby();
        }
        log.error("Received Replication Status Query in Incorrect Role {}.", localClusterDescriptor.getRole());
        return null;
    }

    @Override
    public UUID forceSnapshotSync(String clusterId) throws LogReplicationDiscoveryServiceException {
        if (localClusterDescriptor.getRole() == ClusterRole.STANDBY) {
            String errorStr = "The forceSnapshotSync command is not supported on standby cluster.";
            log.error(errorStr);
            throw new LogReplicationDiscoveryServiceException(errorStr);
        }

        UUID forceSyncId = UUID.randomUUID();
        log.info("Received forceSnapshotSync command for standby cluster {}, forced sync id {}",
                clusterId, forceSyncId);

        // Write a force sync event to the logReplicationEventTable
        ReplicationEventKey key = ReplicationEventKey.newBuilder().setKey(System.currentTimeMillis() + " " + clusterId).build();
        ReplicationEvent event = ReplicationEvent.newBuilder()
                .setClusterId(clusterId)
                .setEventId(forceSyncId.toString())
                .setType(ReplicationEvent.ReplicationEventType.FORCE_SNAPSHOT_SYNC)
                .build();
        getLogReplicationMetadataManager().updateLogReplicationEventTable(key, event);
        return forceSyncId;
    }

    @Override
    public ClusterRole getLocalClusterRoleType() {
        return localClusterDescriptor.getRole();
    }

    public void shutdown() {
        if (logReplicationEventListener != null) {
            logReplicationEventListener.stop();
        }

        if (replicationManager != null) {
            replicationManager.stop();
        }

        if (clusterManagerAdapter != null) {
            clusterManagerAdapter.shutdown();
        }

        if (lockClient != null) {
            lockClient.shutdown();
        }

        if (logReplicationMetadataManager != null) {
            logReplicationMetadataManager.shutdown();
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


    private Optional<LongTaskTimer.Sample> recordLockAcquire(ClusterRole role) {
        return MeterRegistryProvider.getInstance()
                .map(registry -> registry.more()
                        .longTaskTimer("logreplication.lock.duration",
                                ImmutableList.of(
                                        Tag.of("cluster.role", role.toString().toLowerCase())))
                        .start());
    }

    private void processCountOnLockAcquire(ClusterRole role) {
        MeterRegistryProvider.getInstance()
                .ifPresent(registry -> {
                    if (!lockAcquisitionsByRole.containsKey(role)) {
                        AtomicLong numAcquisitions = registry.gauge("logreplication.lock.acquire.count",
                                ImmutableList.of(Tag.of("cluster.role", role.toString().toLowerCase())),
                                new AtomicLong(0));
                        lockAcquisitionsByRole.put(role, numAcquisitions);
                    }
                    lockAcquisitionsByRole.computeIfPresent(role, (r, numAcquisitions) -> {
                        numAcquisitions.getAndIncrement();
                        return numAcquisitions;
                    });
                });
    }

    private void recordLockRelease() {
        lockAcquireSample.ifPresent(LongTaskTimer.Sample::stop);
    }

    private void updateReplicationStatus() {
        if (!statusFlag) {
            replicationManager.updateStatusAsNotStarted();
            statusFlag = true;
        }
    }

    private void setupLocalNodeId() {
        // Retrieve system-specific node id
        LogReplicationPluginConfig config = new LogReplicationPluginConfig(serverContext.getPluginConfigFilePath());
        String nodeIdFilePath = config.getNodeIdFilePath();
        if (nodeIdFilePath != null) {
            File nodeIdFile = new File(nodeIdFilePath);
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(nodeIdFile))) {
                String line = bufferedReader.readLine();
                localNodeId = line.split("=")[1].trim().toLowerCase();
                log.info("setupLocalNodeId succeeded, node id is {}", localNodeId);
            } catch (IOException e) {
                log.error("setupLocalNodeId failed", e);
                throw new IllegalStateException(e.getCause());
            }
        } else {
            log.error("setupLocalNodeId failed, because nodeId file path is missing!");
            // For testing purpose, it uses the default host to assign node id
            if (getLocalHost().equals(DefaultClusterConfig.getDefaultHost())) {
                localNodeId = DefaultClusterConfig.getDefaultNodeId(localEndpoint);
                if (localNodeId != null) {
                    log.info("setupLocalNodeId failed, using default node id {} for test", localNodeId);
                } else {
                    throw new IllegalStateException("SetupLocalNodeId failed for testing");
                }
            } else {
                throw new IllegalArgumentException("NodeId file path is missing");
            }
        }
    }

    private void resetReplicationStatusTableWithRetry() {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    logReplicationMetadataManager.resetReplicationStatus();
                } catch (TransactionAbortedException tae) {
                    log.error("Error while attempting to resetReplicationStatusTable in DiscoveryService's role change", tae);
                    throw new RetryNeededException();
                }

                log.debug("resetReplicationStatusTable succeeds");
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to resetReplicationStatusTable in DiscoveryService's role change.", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }
}
