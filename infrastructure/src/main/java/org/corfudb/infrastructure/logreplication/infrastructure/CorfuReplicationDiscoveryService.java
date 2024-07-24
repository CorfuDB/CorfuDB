package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Tag;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.config.ConfigParamNames;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.LogReplicationServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.PgUtils.PostgresConnector;
import org.corfudb.infrastructure.logreplication.PostgresReplicationConfig;
import org.corfudb.infrastructure.logreplication.PostgresReplicationConnectionConfig;
import org.corfudb.infrastructure.logreplication.ReplicationConfig;
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
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.corfudb.common.util.URLUtils.getHostFromEndpointURL;
import static org.corfudb.common.util.URLUtils.getVersionFormattedEndpointURL;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.createPublicationCmd;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.createSubscriptionCmd;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.createTablesCmds;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.dropAllSubscriptions;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.dropPublications;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.dropSubscriptions;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.getAllPublications;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.getAllSubscriptions;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.getPgReplicationStatus;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.makeTablesReadOnly;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.makeTablesWriteable;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.truncateTables;
import static org.corfudb.infrastructure.logreplication.PgUtils.PostgresUtils.tryExecuteCommand;
import static org.corfudb.infrastructure.logreplication.PostgresReplicationConnectionConfig.isPostgres;

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
     * Used for managing the set of streams to replicate, and also used for upgrading path
     * in Log Replication
     */
    private LogReplicationConfigManager replicationConfigManager;

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
     * This is the listener to the replication event table shared by the nodes in the cluster.
     * When a non-leader node is called to do the enforcedSnapshotSync, it will write the event to
     * the shared event-table and the leader node will be notified to do the work.
     */
    private LogReplicationEventListener logReplicationEventListener;

    private PostgresConnector connector;

    private final PostgresReplicationConnectionConfig pgConfig;

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
        this.pgConfig = new PostgresReplicationConnectionConfig(serverContext.getPluginConfigFilePath());
        if (isPostgres) {
            this.connector = new PostgresConnector(getLocalHost(), pgConfig.getPORT(),
                    pgConfig.getUSER(), pgConfig.getPASSWORD(), pgConfig.getDB_NAME());
        }
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

            if (interClusterReplicationService != null) {
                interClusterReplicationService.close();
            }
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
    private void bootstrapLogReplicationService(TopologyDescriptor topology) {
        log.info("Bootstrap the Log Replication Service");
        if (isPostgres) {
            // Through LogReplicationConfigAdapter retrieve system-specific configurations
            // such as streams to replicate and version
            log.info("Starting with Postgres");
            ReplicationConfig logReplicationConfig = getLogReplicationConfiguration();

            // OPEN TABLES FOR ALL ROLES
            createTables(logReplicationConfig);

            if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE) {
                // TODO (Postgres): This needs to be adapted for clustered, publications only on leader
                // Make tables writeable
                makeTablesWriteable(new ArrayList<>(logReplicationConfig.getStreamsToReplicate()), connector);
                if (!tryExecuteCommand(createPublicationCmd(logReplicationConfig.getStreamsToReplicate(), connector), connector)) {
                    return;
                }
                log.info("CREATED PUBLICATIONS");
            } else if (localClusterDescriptor.getRole() == ClusterRole.STANDBY) {
                dropAllSubscriptions(connector);
                truncateTables(new ArrayList<>(logReplicationConfig.getStreamsToReplicate()), connector);
                makeTablesReadOnly(new ArrayList<>(logReplicationConfig.getStreamsToReplicate()), connector);
                log.info("Cleared tables to be replicated!");

                AtomicBoolean success = new AtomicBoolean(true);
                topology.getActiveClusters().values().forEach(
                        activeCluster -> activeCluster.getNodesDescriptors().forEach(
                                nodeDescriptor -> {
                                    String activeNodeIp = nodeDescriptor.getHost().split(":")[0];
                                    log.info("Trying to connect to remote host: {}", activeNodeIp);
                                    PostgresConnector activeConnector = new PostgresConnector(activeNodeIp,
                                            pgConfig.getREMOTE_PG_PORT(), pgConfig.getUSER(), pgConfig.getPASSWORD(), pgConfig.getDB_NAME());

                                    if (!tryExecuteCommand(createSubscriptionCmd(activeConnector, connector), connector)) {
                                        log.error("bootstrapLogReplicationService: Error while subscribing to new remote active {} ", activeNodeIp);
                                        success.set(false);
                                    } else {
                                        success.set(true);
                                    }
                                })
                );

                if (!success.get()) {
                    return;
                }
                log.info("CREATED SUBSCRIPTIONS");
            }

        } else {
            // Through LogReplicationConfigAdapter retrieve system-specific configurations
            // such as streams to replicate and version
            ReplicationConfig logReplicationConfig = getLogReplicationConfiguration(getCorfuRuntime());

            logReplicationMetadataManager = new LogReplicationMetadataManager(getCorfuRuntime(),
                    topologyDescriptor.getTopologyConfigId(), localClusterDescriptor.getClusterId());

            logReplicationServerHandler = new LogReplicationServer(serverContext, (LogReplicationConfig) logReplicationConfig,
                    logReplicationMetadataManager, localCorfuEndpoint, topologyDescriptor.getTopologyConfigId(), localNodeId);
            if (localClusterDescriptor.getRole().equals(ClusterRole.ACTIVE)) {
                logReplicationServerHandler.setActive(true);
                addDefaultReplicationStatus();
            } else if (localClusterDescriptor.getRole().equals(ClusterRole.STANDBY)) {
                logReplicationServerHandler.setStandby(true);
            }

            interClusterReplicationService = new CorfuInterClusterReplicationServerNode(
                    serverContext,
                    ImmutableMap.<Class, AbstractServer>builder()
                            .put(BaseServer.class, new BaseServer(serverContext))
                            .put(LogReplicationServer.class, logReplicationServerHandler)
                            .build());

            // Pass server's channel context through the Log Replication Context, for shared objects between the server
            // and the client channel (specific requirements of the transport implementation)
            replicationContext = new LogReplicationContext((LogReplicationConfig) logReplicationConfig, topologyDescriptor,
                    localCorfuEndpoint, interClusterReplicationService.getRouter().getServerAdapter().getChannelContext());

            // Unblock server initialization & register to Log Replication Lock, to attempt lock / leadership acquisition
            serverCallback.complete(interClusterReplicationService);

            logReplicationEventListener = new LogReplicationEventListener(this);
        }

        serverStarted = true;
    }

    public void createTables(ReplicationConfig replicationConfig) {
        Stack<String> tablesToCreateCmds = new Stack<>();
        tablesToCreateCmds.addAll(createTablesCmds(replicationConfig.getStreamsToReplicate()));

        while (!tablesToCreateCmds.empty()) {
            try {
                IRetry.build(IntervalRetry.class, () -> {
                    try {
                        if(tryExecuteCommand(tablesToCreateCmds.peek(), connector)) {
                            tablesToCreateCmds.pop();
                        }
                    } catch (Exception e) {
                        log.error("Error while attempting to create table", e);
                        throw new RetryNeededException();
                    }
                    return null;
                }).run();
            } catch (InterruptedException e) {
                log.error("Unable to create all tables", e);
                throw new UnrecoverableCorfuInterruptedError(e);
            }
        }
    }

    /**
     * Retrieve a Corfu Runtime to connect to the local Corfu Datastore.
     */
    private CorfuRuntime getCorfuRuntime() {
        // Avoid multiple runtime's
        if (runtime == null) {
            log.debug("Connecting to local Corfu {}", localCorfuEndpoint);
            runtime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder()
                    .trustStore((String) serverContext.getServerConfig().get(ConfigParamNames.TRUST_STORE))
                    .tsPasswordFile((String) serverContext.getServerConfig().get(ConfigParamNames.TRUST_STORE_PASS_FILE))
                    .keyStore((String) serverContext.getServerConfig().get(ConfigParamNames.KEY_STORE))
                    .ksPasswordFile((String) serverContext.getServerConfig().get(ConfigParamNames.KEY_STORE_PASS_FILE))
                    .tlsEnabled((Boolean) serverContext.getServerConfig().get("--enable-tls"))
                    .systemDownHandler(() -> System.exit(SYSTEM_EXIT_ERROR_CODE))
                    // This runtime is used for the LockStore, Metadata Manager and Log Entry Sync, which don't rely
                    // heavily on the cache (hence can be smaller)
                    .maxCacheEntries(serverContext.getLogReplicationCacheMaxSize()/2)
                    .maxWriteSize(serverContext.getMaxWriteSize())
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
    private String getCorfuEndpoint(String localHostAddress, int corfuPort) {
        return getVersionFormattedEndpointURL(localHostAddress, corfuPort);
    }

    /**
     * Retrieve Log Replication Configuration.
     * <p>
     * This configuration represents all common parameters for the log replication, regardless of
     * a cluster's role.
     */
    private ReplicationConfig getLogReplicationConfiguration(CorfuRuntime runtime) {

        try {
            replicationConfigManager =
                new LogReplicationConfigManager(runtime, serverContext.getPluginConfigFilePath());

            return new LogReplicationConfig(
                    replicationConfigManager,
                    serverContext.getLogReplicationMaxNumMsgPerBatch(),
                    serverContext.getLogReplicationMaxDataMessageSize(),
                    serverContext.getLogReplicationCacheMaxSize(),
                    serverContext.getMaxSnapshotEntriesApplied());
        } catch (Throwable t) {
            log.error("Exception when fetching the Replication Config", t);
            throw t;
        }
    }

    private ReplicationConfig getLogReplicationConfiguration() {
        try {
            replicationConfigManager =
                    new LogReplicationConfigManager(serverContext.getPluginConfigFilePath());
            return new PostgresReplicationConfig(replicationConfigManager, pgConfig);
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
        if (!isPostgres) {
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
                            getCorfuRuntime(), replicationConfigManager);
                }
                logReplicationEventListener.start();
                replicationManager.setTopology(topologyDescriptor);
                replicationManager.start();
                lockAcquireSample = recordLockAcquire(localClusterDescriptor.getRole());
                processCountOnLockAcquire(localClusterDescriptor.getRole());
                break;
            case STANDBY:
                // Standby Site : the LogReplicationServer (server handler) will initiate the LogReplicationSinkManager
                log.info("Start as Sink (receiver)");
                interClusterReplicationService.getLogReplicationServer().getSinkManager().reset();
                interClusterReplicationService.getLogReplicationServer().setLeadership(true);
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
        logReplicationEventListener.stop();
        stopLogReplication();
        isLeader.set(false);
        // Signal Log Replication Server/Sink to stop receiving messages, leadership loss
        interClusterReplicationService.getLogReplicationServer().setLeadership(false);
        interClusterReplicationService.getLogReplicationServer().stopSink();
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

        log.info("OnClusterRoleChange, topology={}, localClusterDescriptor={}", newTopology, localClusterDescriptor);

        if (isPostgres) {
            ReplicationConfig logReplicationConfig = getLogReplicationConfiguration();

            if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE && newTopology.getClusterDescriptor(localNodeId).getRole() == ClusterRole.STANDBY) {
                log.info("Role change happening from Active to Standby!");

                log.info("Current Standby Clusters {}", topologyDescriptor.getStandbyClusters());
                //  Drop all subscriptions from current standby to current active clusters
                topologyDescriptor.getStandbyClusters().values().forEach(
                        clusterDescriptor -> clusterDescriptor.getNodesDescriptors().forEach(
                                nodeDescriptor -> {
                                    String standbyNodeIp = nodeDescriptor.getHost().split(":")[0];
                                    log.info("onClusterRoleChange: Trying to connect to remote host to drop subscriptions: {}", standbyNodeIp);

                                    PostgresConnector standbyConnector = new PostgresConnector(standbyNodeIp,
                                            pgConfig.getREMOTE_PG_PORT(), pgConfig.getUSER(), pgConfig.getPASSWORD(), pgConfig.getDB_NAME());

                                    dropSubscriptions(getAllSubscriptions(standbyConnector), standbyConnector);

                                    // For safety, check and wait until all the subscriptions are dropped
                                    while (!getAllSubscriptions(standbyConnector).isEmpty()) {
                                        try {
                                            TimeUnit.SECONDS.sleep(5);
                                        } catch (InterruptedException e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                })
                );

                log.info("Subscriptions are dropped on replica, dropping inactive publications on active");

                // Drop all publications and replication slots
                dropPublications(getAllPublications(connector), connector);

                // Clear tables
                truncateTables(new ArrayList<>(logReplicationConfig.getStreamsToReplicate()), connector);
                makeTablesReadOnly(new ArrayList<>(logReplicationConfig.getStreamsToReplicate()), connector);

                // Subscribe to new publications if there is any active node in the new topology
                // mostly this is a no-op as there are no active clusters in topology at this point.
                // Both the clusters are standby.
                AtomicBoolean success = new AtomicBoolean(true);
                newTopology.getActiveClusters().values().forEach(
                        activeCluster -> activeCluster.getNodesDescriptors().forEach(
                                nodeDescriptor -> {
                                    String activeNodeIp = nodeDescriptor.getHost().split(":")[0];
                                    log.info("onClusterRoleChange: Trying to connect to remote active to create subscriptions from the old active, now standby: {}", activeNodeIp);
                                    PostgresConnector activeConnector = new PostgresConnector(activeNodeIp,
                                            pgConfig.getREMOTE_PG_PORT(), pgConfig.getUSER(), pgConfig.getPASSWORD(), pgConfig.getDB_NAME());

                                    if (!tryExecuteCommand(createSubscriptionCmd(activeConnector, connector), connector)) {
                                        log.error("onClusterRoleChange: Error while subscribing to new remote active {} ", activeNodeIp);
                                        success.set(false);
                                    } else {
                                        success.set(true);
                                    }
                                })
                );

                if (!success.get()) {
                    return;
                }
                log.info("TRANSFORMED ACTIVE TO STANDBY");
            } else if (localClusterDescriptor.getRole() == ClusterRole.STANDBY && newTopology.getClusterDescriptor(localNodeId).getRole() == ClusterRole.ACTIVE) {
                log.info("Role change happening from Standby to Active!");

                // Make tables writeable
                makeTablesWriteable(new ArrayList<>(logReplicationConfig.getStreamsToReplicate()), connector);

                List<String> subscriptionsToDrop = getAllSubscriptions(connector);
                dropSubscriptions(subscriptionsToDrop, connector);

                if (!tryExecuteCommand(createPublicationCmd(logReplicationConfig.getStreamsToReplicate(), connector), connector)) {
                    return;
                }

                // Create subscriptions on new remote standby to new local active
                AtomicBoolean success = new AtomicBoolean(true);
                newTopology.getStandbyClusters().values().forEach(
                        standbyCluster -> standbyCluster.getNodesDescriptors().forEach(
                                nodeDescriptor -> {
                                    String standbyNodeIp = nodeDescriptor.getHost().split(":")[0];
                                    log.info("onClusterRoleChange: Trying to connect to remote standby {} to create subscriptions to new local active {}.", standbyNodeIp, connector.ADDRESS);
                                    PostgresConnector standbyConnector = new PostgresConnector(standbyNodeIp,
                                            pgConfig.getREMOTE_PG_PORT(), pgConfig.getUSER(), pgConfig.getPASSWORD(), pgConfig.getDB_NAME());

                                    if (!tryExecuteCommand(createSubscriptionCmd(connector, standbyConnector), standbyConnector)) {
                                        log.error("bootstrapLogReplicationService: Error while subscribing to new remote active {} ", standbyNodeIp);
                                        success.set(false);
                                    } else {
                                        success.set(true);
                                    }
                                })
                );
                if (!success.get()) {
                    return;
                }
                log.info("Created subscriptions on the standby nodes to the new active cluster");

                log.info("TRANSFORMED STANDBY TO ACTIVE");
            } else if (localClusterDescriptor.getRole() == ClusterRole.NONE && newTopology.getClusterDescriptor(localNodeId).getRole() == ClusterRole.STANDBY) {
                log.info("Role change happening from None to Standby! No op as its taken care by onStandbyClusterAddRemove triggered on the Active Cluster.");
            } else if (localClusterDescriptor.getRole() == ClusterRole.NONE && newTopology.getClusterDescriptor(localNodeId).getRole() == ClusterRole.ACTIVE) {
                log.info("Role change happening from None to Active!");
                // Make tables writeable
                makeTablesWriteable(new ArrayList<>(logReplicationConfig.getStreamsToReplicate()), connector);

                List<String> subscriptionsToDrop = getAllSubscriptions(connector);
                dropSubscriptions(subscriptionsToDrop, connector);

                if (!tryExecuteCommand(createPublicationCmd(logReplicationConfig.getStreamsToReplicate(), connector), connector)) {
                    return;
                }

                log.info("TRANSFORMED NONE TO ACTIVE");
            }

            // Update topology, cluster, and node configs
            updateLocalTopology(newTopology);
        } else {
            // Stop ongoing replication, stopLogReplication() checks leadership and active
            // We do not update topology until we successfully stop log replication
            if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE) {
                stopLogReplication();
                logReplicationEventListener.stop();
            }

            // Update topology, cluster, and node configs
            updateLocalTopology(newTopology);

            // Update topology config id in metadata manager
            logReplicationMetadataManager.setupTopologyConfigId(topologyDescriptor.getTopologyConfigId());

            if (isLeader.get()) {
                // Clear the Replication Status on the leader node only.
                // Consider the case of async configuration changes, non-lead nodes could overwrite
                // the replication status if it has already completed by the lead node
                // Note: For the cluster which was Active, this clear can race with the async 'replication stop'
                // (stopLogReplication()) which updates the status table with STOPPED state.  So the status table
                // can have 2 entries - 1 from the new Standby State showing the dataConsistent flag, and the other from
                // the time it was Active, showing the replication status as 'STOPPED'.
                // For example:
                // Key:
                //{
                //  "clusterId": "b4c1ae3a-528a-4677-9f15-4a213ffd0da8"
                //}
                //
                //Payload:
                //{
                //  "dataConsistent": true,
                //  "status": "UNAVAILABLE"
                //}
                //                      and
                //Key:
                //{
                //  "clusterId": "3c1bf6c7-70bd-4ad6-8a4f-7b4e5181ddb1"
                //}
                //
                //Payload:
                //{
                //  "syncType": "LOG_ENTRY",
                //  "status": "STOPPED",
                //  "snapshotSyncInfo": {
                //    "type": "FORCED",
                //    "status": "COMPLETED",
                //    "snapshotRequestId": "3671bdb0-d5ec-46cc-9c87-d1a9a2436083",
                //    "completedTime": "2024-03-20T19:11:23.431973Z",
                //    "baseSnapshot": "1575363"
                //  }
                //}
                // This is a known limitation which can be ignored and does not have any functional impact.
                // The limitation is because conflict detection between putRecord()(which sets the status to 'STOPPED') and
                // clear() (clear of the table) is currently not available.
                resetReplicationStatusTableWithRetry();
                // In the event of Standby -> Active we should add the default replication values
                if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE) {
                    addDefaultReplicationStatus();
                }
            }

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
            logReplicationServerHandler.setStandby(localClusterDescriptor.getRole().equals(ClusterRole.STANDBY));

            // On Topology Config Change, only if this node is the leader take action
            if (isLeader.get()) {
                onLeadershipAcquire();
            }
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

        // if (event.getTopologyConfig().getTopologyConfigID() == topologyDescriptor.getTopologyConfigID()){
        //     log.debug("Repeated Topology Change Notification, current={}, received={}",
        //             topologyDescriptor.getTopologyConfigId(), event.getTopologyConfig().getTopologyConfigID());
        //     return;
        // }

        log.debug("Received topology change, topology={}", event.getTopologyConfig());

        TopologyDescriptor discoveredTopology = new TopologyDescriptor(event.getTopologyConfig());

        boolean isValid;
        try {
            isValid = processDiscoveredTopology(discoveredTopology, localClusterDescriptor == null);
            log.info("Found valid topology, topology={}, isValid {}, localClusterDescriptor {}", discoveredTopology, isValid, localClusterDescriptor);
        } catch (Throwable t) {
            log.error("Exception when processing the discovered topology", t);
            stopLogReplication();
            return;
        }

        if (isValid) {
            if (isClusterRoleChanged(discoveredTopology)) {
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
    private boolean isClusterRoleChanged(TopologyDescriptor discoveredTopology) {
        if (localClusterDescriptor != null) {
            log.info("isClusterRoleChanged: discoveredTopology {}," +
                            " localClusterDescriptor.getRole() {}," +
                            " discoveredTopology.getClusterDescriptor(localNodeId).getRole() {}",
                    discoveredTopology,
                    localClusterDescriptor.getRole(),
                    discoveredTopology.getClusterDescriptor(localNodeId).getRole());
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
        log.debug("Standby Cluster has been added or removed");

        if (isPostgres) {
            if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE) {
                log.info("onStandbyClusterAddRemove: Standby added or removed...");

                // ConfigId mismatch could happen if customized cluster manager does not follow protocol
                if (discoveredTopology.getTopologyConfigId() != topologyDescriptor.getTopologyConfigId()) {
                    log.warn("onStandbyClusterAddRemove: Detected changes in the topology. The new topology descriptor {} doesn't have the same " +
                            "topologyConfigId as the current one {}", discoveredTopology, topologyDescriptor);
                }

                Set<String> currentStandbys = new HashSet<>(topologyDescriptor.getStandbyClusters().keySet());
                Set<String> newStandbys = new HashSet<>(discoveredTopology.getStandbyClusters().keySet());
                Set<String> intersection = Sets.intersection(currentStandbys, newStandbys);

                Set<String> standbysToRemove = new HashSet<>(currentStandbys);
                standbysToRemove.removeAll(intersection);

                log.info("onStandbyClusterAddRemove: Standbys to remove: {}", standbysToRemove);

                // Remove standbys that are not in the new config
                for (String clusterId : standbysToRemove) {
                    String remoteNodeIp = topologyDescriptor.getStandbyClusters().get(clusterId).getNodesDescriptors().stream().findAny().get().getHost().split(":")[0];
                    log.info("onStandbyClusterAddRemove: Dropping subscriptions present on old standbysToRemove: {}", remoteNodeIp);

                    PostgresConnector standByConnector = new PostgresConnector(remoteNodeIp,
                            pgConfig.getREMOTE_PG_PORT(), pgConfig.getUSER(), pgConfig.getPASSWORD(), pgConfig.getDB_NAME());

                    dropAllSubscriptions(standByConnector);
                    makeTablesWriteable(new ArrayList<>(getLogReplicationConfiguration().getStreamsToReplicate()), standByConnector);
                    topologyDescriptor.removeStandbyCluster(clusterId);
                }

                for (String clusterId : newStandbys) {
                    if (!topologyDescriptor.getStandbyClusters().containsKey(clusterId)) {
                        String standByNodeIp = discoveredTopology.getStandbyClusters().get(clusterId).getNodesDescriptors().stream().findAny().get().getHost().split(":")[0];
                        log.info("onStandbyClusterAddRemove: Standbys to add: clusterId {}, standByNodeIp: {}", clusterId, standByNodeIp);

                        PostgresConnector standByConnector = new PostgresConnector(standByNodeIp,
                                pgConfig.getREMOTE_PG_PORT(), pgConfig.getUSER(), pgConfig.getPASSWORD(), pgConfig.getDB_NAME());

                        ReplicationConfig logReplicationConfig = getLogReplicationConfiguration();

                        dropAllSubscriptions(standByConnector);
                        truncateTables(new ArrayList<>(logReplicationConfig.getStreamsToReplicate()), standByConnector);
                        makeTablesReadOnly(new ArrayList<>(logReplicationConfig.getStreamsToReplicate()), standByConnector);
                        log.info("onStandbyClusterAddRemove: Cleared tables to be replicated on clusterId {}, node {}", clusterId, standByNodeIp);

                        AtomicBoolean success = new AtomicBoolean(true);
                        // Start the standbys that are in the new config but not in the old config
                        log.info("onStandbyClusterAddRemove: Starting subscriptions on new standBy: {} to active", standByNodeIp);
                        discoveredTopology.getActiveClusters().values().forEach(
                                activeCluster -> activeCluster.getNodesDescriptors().forEach(
                                        nodeDescriptor -> {
                                            String activeNodeIp = nodeDescriptor.getHost().split(":")[0];
                                            log.info("onStandbyClusterAddRemove: Trying to connect to remote active host: {}", activeNodeIp);
                                            PostgresConnector activeConnector = new PostgresConnector(activeNodeIp,
                                                    pgConfig.getREMOTE_PG_PORT(), pgConfig.getUSER(), pgConfig.getPASSWORD(), pgConfig.getDB_NAME());

                                            if (!tryExecuteCommand(createSubscriptionCmd(activeConnector, standByConnector), standByConnector)) {
                                                log.error("onStandbyClusterAddRemove: Error while subscribing to new remote active {} ", activeNodeIp);
                                                success.set(false);
                                            } else {
                                                success.set(true);
                                            }
                                        })
                        );

                        if (!success.get()) {
                            return;
                        }
                        log.info("onStandbyClusterAddRemove: Created Subscriptions");
                        ClusterDescriptor clusterInfo = discoveredTopology.getStandbyClusters().get(clusterId);
                        topologyDescriptor.addStandbyCluster(clusterInfo);
                    }
                }
            }
            updateLocalTopology(discoveredTopology);
        } else {
            // We only need to process new standby's if your role is of an ACTIVE cluster
            if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE && replicationManager != null && isLeader.get()) {
                replicationManager.processStandbyChange(discoveredTopology);
            }
            updateLocalTopology(discoveredTopology);
            updateReplicationManagerTopology(discoveredTopology);
            updateTopologyConfigId(topologyDescriptor.getTopologyConfigId());
        }

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
                bootstrapLogReplicationService(topology);
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

    /**
     * Enforce a snapshot full sync for all standbys if the current node is a leader node
     */
    private void processEnforceSnapshotSync(DiscoveryServiceEvent event) {
        if (!isPostgres) {
            if (replicationManager == null || !isLeader.get()) {
                log.warn("The current node is not the leader, will skip doing the " +
                        "forced snapshot sync with id {}", event.getEventId());
                return;
            }

            replicationManager.enforceSnapshotSync(event);
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
     * <p>
     * Standby Cluster - Read the shared metadata table and find if data is consistent(returns false if
     * snapshot sync is in the apply phase)
     */
    @Override
    public Map<String, LogReplicationMetadata.ReplicationStatusVal> queryReplicationStatus() {
        if (isPostgres) {
            Map<String, ReplicationStatusVal> mapToSend = new HashMap<>();
            if (localClusterDescriptor != null && localClusterDescriptor.getRole() == ClusterRole.ACTIVE) {
                mapToSend.put(localClusterDescriptor.clusterId, getPgReplicationStatus(connector));
            }
            return mapToSend;
        } else {
            if (localClusterDescriptor == null || logReplicationMetadataManager == null) {
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
    }

    @Override
    public UUID forceSnapshotSync(String clusterId) throws LogReplicationDiscoveryServiceException {
        if (localClusterDescriptor.getRole() == ClusterRole.STANDBY) {
            String errorStr = "The forceSnapshotSync command is not supported on standby cluster.";
            log.error(errorStr);
            throw new LogReplicationDiscoveryServiceException(errorStr);
        } else if (isPostgres) {
            String errorStr = "The forceSnapshotSync command is not supported on Postgres cluster.";
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
        return getHostFromEndpointURL(serverContext.getLocalEndpoint());
    }

    /**
     * Return IP for active node
     * TODO (Postgres): assumes single node, no leadership consensus
     *
     * @return active node's IP
     */
    private String getStandbyNodeHost(TopologyDescriptor descriptor) {
        Map<String, ClusterDescriptor> standbyClusters = descriptor.getStandbyClusters();
        String standByNodeHost = standbyClusters.values().stream().findAny().get()
                .getNodesDescriptors().stream().findAny().get().getHost();
        return standByNodeHost.split(":")[0];
    }


    private Optional<LongTaskTimer.Sample> recordLockAcquire(ClusterRole role) {
        return MeterRegistryProvider.getInstance()
                .map(registry -> registry.more()
                        .longTaskTimer("logreplication.lock.duration.nanoseconds",
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

    private void addDefaultReplicationStatus() {
        // Add default entry to Replication Status Table
        for (ClusterDescriptor clusterDescriptor : topologyDescriptor.getStandbyClusters().values()) {
            logReplicationMetadataManager.initializeReplicationStatusTable(clusterDescriptor.clusterId);
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
