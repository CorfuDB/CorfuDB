package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Tag;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.config.ConfigParamNames;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
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
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkServerRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceServerRouter;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
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
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * This class represents the Log Replication Discovery Service.
 * <p>
 * It manages the following:
 * <p>
 * - Discover topology and determine cluster role: source/sink
 * - Lock acquisition (leader election), node leading the log replication sending and receiving
 * - Log replication configuration (streams to replicate)
 */
@Slf4j
public class CorfuReplicationDiscoveryService implements CorfuReplicationDiscoveryServiceAdapter {
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
    private final Map<ReplicationSession, LogReplicationMetadataManager> remoteSessionToMetadataManagerMap = new HashMap<>();

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
     * Used by the source cluster to initiate Log Replication
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

    private CorfuInterClusterReplicationServerNode interClusterServerNode;

    private final ServerContext serverContext;

    private String localCorfuEndpoint;

    private CorfuRuntime runtime;

    private LogReplicationContext replicationContext;

    private boolean shouldRun = true;

    @Getter
    private final AtomicBoolean isLeader;

    private LockClient lockClient;

    private LogReplicationConfig logReplicationConfig;

    /**
     * Indicates that bootstrap has been completed. Bootstrap is done once it
     * is determined that this node belongs to a cluster in the topology
     * provided by ClusterManager and has the role of SOURCE or SINK
     */
    private boolean bootstrapComplete = false;

    /**
     * This is the listener to the replication event table shared by the nodes in the cluster.
     * When a non-leader node is called to do the enforcedSnapshotSync, it will write the event to
     * the shared event-table and the leader node will be notified to do the work.
     */
    private LogReplicationEventListener logReplicationEventListener;

    /**
     * Used to create router and initiate connection to eligible remotes.
     */
    // TODO: this will be removed once we have the session manager
    private final Map<String, Set<ReplicationSession>> remoteClusterIdToReplicationSession = new HashMap<>();


    private final Map<Class, AbstractServer> serverMap = new HashMap<>();

    /**
     * Constructor Discovery Service
     *
     * @param serverContext         current server's context
     */
    public CorfuReplicationDiscoveryService(@Nonnull ServerContext serverContext) {
        this.serverContext = serverContext;
        this.logReplicationLockId = serverContext.getNodeId();
        this.localEndpoint = serverContext.getLocalEndpoint();
        this.isLeader = new AtomicBoolean();
        this.clusterManagerAdapter = getClusterManagerAdapter(serverContext.getPluginConfigFilePath());
    }

    /**
     * Create the Cluster Manager Adapter, i.e., the adapter to external provider of the topology.
     * @param pluginConfigFilePath File path of the ClusterManagerAdapter plugin
     * @return cluster manager adapter instance
     */
    private CorfuReplicationClusterManagerAdapter getClusterManagerAdapter(String pluginConfigFilePath) {

        LogReplicationPluginConfig config = new LogReplicationPluginConfig(pluginConfigFilePath);
        File jar = new File(config.getTopologyManagerAdapterJARPath());

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class adapter = Class.forName(config.getTopologyManagerAdapterName(), true, child);
            return (CorfuReplicationClusterManagerAdapter) adapter.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            log.error("Fatal error: Failed to create serverAdapter", e);
            throw new UnrecoverableCorfuError(e);
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

            if (interClusterServerNode != null) {
                interClusterServerNode.close();
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
     * (streams to replicate) required by an source and sink site before starting
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
     * Instantiate the LR components based on role
     * Source:
     * - build logReplication context(LR context available for both Source and Sink.  Currently only used on the Source)
     * - start even listener: listens to forced snapshot sync requests
     * Sink:
     * - Start Log Replication Server(listens and processes incoming requests from the Source)
     * Both:
     * - Metadata Managers which maintain metadata related to replication and its status
     * @param topology
     */
    private void performRoleBasedSetups(TopologyDescriptor topology) {
        if (topology.getRemoteSourceClusters().isEmpty() && topology.getRemoteSinkClusters().isEmpty()) {
            log.debug("There is no remote-source/remote-sink for this clusters. Not performing role-based setup.");
            return;
        }

        // Through the config manager, retrieve system-specific configurations such as streams to replicate
        // for supported replication models and version
        replicationConfigManager = new LogReplicationConfigManager(getCorfuRuntime(), serverContext);
        logReplicationConfig = replicationConfigManager.getConfig();
        replicationContext = new LogReplicationContext(logReplicationConfig, topologyDescriptor, localCorfuEndpoint);

        if (!topology.getRemoteSinkClusters().isEmpty()) {
            setupForSource();
        }
        if (!topology.getRemoteSourceClusters().isEmpty()) {
            setupForSink();
        }

    }

    private void setupForSource() {
        Set<String> remoteSinkClusterIds = new HashSet<>();
        remoteSinkClusterIds.addAll(topologyDescriptor.getRemoteSinkClusters().keySet());
        createMetadataManagers(remoteSinkClusterIds, topologyDescriptor);
        logReplicationEventListener = new LogReplicationEventListener(this, getCorfuRuntime());
        logReplicationEventListener.start();
    }


    private void setupForSink() {
        Set<String> remoteSourceClusterIds = new HashSet<>();
        remoteSourceClusterIds.addAll(topologyDescriptor.getRemoteSourceClusters().keySet());
        createMetadataManagers(remoteSourceClusterIds, topologyDescriptor);
    }

    private void createMetadataManagers(Set<String> remoteClusterIds, TopologyDescriptor topology) {
        for (String remoteClusterId : remoteClusterIds) {
            // for testing purpose
            if (remoteClusterId == localClusterDescriptor.getClusterId()) {
                continue;
            }
            // TODO: this will be replaced by session manager
            Map<ClusterDescriptor, Set<LogReplication.ReplicationModel>> remoteClusterToReplicationModel = new HashMap<>();
            remoteClusterToReplicationModel.putAll(topology.getRemoteSinkClusterToReplicationModels());
            remoteClusterToReplicationModel.putAll(topology.getRemoteSourceClusterToReplicationModels());

            for (Map.Entry<ClusterDescriptor, Set<LogReplication.ReplicationModel>> e : remoteClusterToReplicationModel.entrySet()) {
                remoteClusterIdToReplicationSession.putIfAbsent(remoteClusterId, new HashSet<>());
                // session creation
                e.getValue().forEach(model -> {
                    ReplicationSession replicationSession = new ReplicationSession(remoteClusterId,localClusterDescriptor.getClusterId(),
                            ReplicationSubscriber.getDefaultReplicationSubscriber());
                    remoteClusterIdToReplicationSession.get(remoteClusterId).add(replicationSession);
                    LogReplicationMetadataManager metadataManager = new LogReplicationMetadataManager(getCorfuRuntime(),
                            topologyDescriptor.getTopologyConfigId(), replicationSession);
                    remoteSessionToMetadataManagerMap.put(replicationSession, metadataManager);
                });
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
                topologyDescriptor = topology;
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
     * Depending on the role of the cluster to which this leader node belongs, it will start
     * as source (sender/producer) or sink (receiver).
     */
    private void onLeadershipAcquire() {
        if (replicationManager == null) {
            replicationManager = new CorfuReplicationManager(replicationContext,
                    localNodeDescriptor, remoteSessionToMetadataManagerMap, serverContext.getPluginConfigFilePath(),
                    getCorfuRuntime(), replicationConfigManager);
        } else {
            // Replication Context contains the topology which must be updated if it has changed
            replicationManager.setContext(replicationContext);
        }

        initReplicationStatusForRemoteClusters(topologyDescriptor.getRemoteSinkClusters().keySet(), true);
        initReplicationStatusForRemoteClusters(topologyDescriptor.getRemoteSourceClusters().keySet(), false);

        Set<ClusterDescriptor> connectionEndpoints = new HashSet<>(fetchConnectionEndpoints().values());
        Set<ClusterDescriptor> remoteSinkClusters = new HashSet<>(topologyDescriptor.getRemoteSinkClusters().values());
        Set<ClusterDescriptor> remoteSourceClusters = new HashSet<>(topologyDescriptor.getRemoteSourceClusters().values());

        log.debug("remoteSourceCluster: {}, remoteSinkCluster: {} , connectionEndpoints: {}",
                topologyDescriptor.getRemoteSourceClusters().values(), topologyDescriptor.getRemoteSinkClusters().values(),
                fetchConnectionEndpoints().values());

        // check if local cluster is a connectionReceiver. This is used to start the server components
        remoteSinkClusters.removeAll(connectionEndpoints);
        remoteSourceClusters.removeAll(connectionEndpoints);
        boolean isConnectionReceiver = !remoteSinkClusters.isEmpty() || !remoteSourceClusters.isEmpty();

        setupConnectionReceiversAndSinkServers(isConnectionReceiver, !topologyDescriptor.getRemoteSourceClusters().isEmpty());

        // create connection endpoint routers to pass to the interClusterServerNode.
        // The incoming msgs will be routed to the appropriate router in the transport layer adapter.
        Map<ReplicationSession, LogReplicationSourceServerRouter> receivedSessionToSourceServer = new HashMap<>();
        Map<ReplicationSession, LogReplicationSinkServerRouter> receivedSessionToSinkServer = new HashMap<>();

        if(!remoteSinkClusters.isEmpty()) {
            startSourceComponents(connectionEndpoints, receivedSessionToSourceServer);
        }

        if (!remoteSourceClusters.isEmpty()) {
            startSinkComponents(connectionEndpoints, receivedSessionToSinkServer);
        }

        if(isConnectionReceiver) {
            interClusterServerNode.setRouterAndStartServer(receivedSessionToSourceServer, receivedSessionToSinkServer);
        }

        // connect to endpoints
        replicationManager.startConnection(connectionEndpoints, remoteClusterIdToReplicationSession, serverMap);

        lockAcquireSample = recordLockAcquire(localClusterDescriptor.getRole());
        processCountOnLockAcquire(localClusterDescriptor.getRole());
    }


    private void setupConnectionReceiversAndSinkServers(boolean isConnectionReceiver, boolean isSink) {
        if(!isConnectionReceiver && !isSink) {
            return;
        }

        LogReplicationServer lrServer = new LogReplicationServer(serverContext, localNodeId, replicationConfigManager,
                localCorfuEndpoint, topologyDescriptor.getTopologyConfigId(), remoteSessionToMetadataManagerMap);
        serverMap.put(LogReplicationServer.class, lrServer);
        serverMap.put(BaseServer.class, new BaseServer(serverContext));


        if (isConnectionReceiver) {
            interClusterServerNode = new CorfuInterClusterReplicationServerNode(serverContext, serverMap);

            // Sink Site : the LogReplicationServer (server handler) will reset the LogReplicationSinkManager on acquiring
            // leadership
            interClusterServerNode.setLeadership(true);
        } else {
            lrServer.setLeadership(true);
        }
    }

    private Map<String, ClusterDescriptor> fetchConnectionEndpoints() {
        Set<LogReplicationClusterInfo.ClusterConfigurationMsg> clusterEndpointMsgs =
                clusterManagerAdapter.fetchConnectionEndpoints().stream()
                        // filter for testing purpose
                        .filter(clusterConfigurationMsg ->
                                clusterConfigurationMsg.getId() != localClusterDescriptor.getClusterId())
                        .collect(Collectors.toSet());
        Map<String, ClusterDescriptor> clusterIdToConnectionEndpoint = new HashMap<>();
         clusterEndpointMsgs.stream().forEach(clusterConfigMsg ->
                 clusterIdToConnectionEndpoint.put(clusterConfigMsg.getId(),new ClusterDescriptor(clusterConfigMsg)));
         return clusterIdToConnectionEndpoint;
    }

    // create SOURCE routers and runtime if the cluster is not a connection starter
    private void startSourceComponents(Set<ClusterDescriptor> connectionEndpointsToLocalCluster,
                                       Map<ReplicationSession, LogReplicationSourceServerRouter> sessionToSourceServer) {
        log.info("Starting Source (sender/replicator) components");

        Set<String> allRemoteSinkClusterIds = new HashSet<>(topologyDescriptor.getRemoteSinkClusters().keySet());
        connectionEndpointsToLocalCluster.stream().forEach(cluster -> allRemoteSinkClusterIds.remove(cluster.clusterId));

        // for testing purpose
        if (allRemoteSinkClusterIds.contains(localClusterDescriptor.clusterId)) {
            allRemoteSinkClusterIds.remove(localClusterDescriptor.clusterId);
        }

        if (!allRemoteSinkClusterIds.isEmpty()) {
            log.debug("starting sources and wait for connection from remote sinks {}", allRemoteSinkClusterIds);
            allRemoteSinkClusterIds.stream().forEach(clusterId -> {
                for (ReplicationSession session : remoteClusterIdToReplicationSession.get(clusterId)) {
                    // create incoming session so that the transport layer is able to route the incoming msgs to the correct router.
                    ReplicationSession incomingSession = new ReplicationSession(
                            localClusterDescriptor.getClusterId(), session.getRemoteClusterId(), session.getSubscriber());
                    sessionToSourceServer.put(incomingSession,
                            (LogReplicationSourceServerRouter) replicationManager.getOrCreateSourceRouter(
                                    topologyDescriptor.getRemoteSinkClusters().get(clusterId),
                                    session, serverMap, false)
                    );
                }
            });
        } else {
            log.debug("No remote cluster found for which this cluster is a connection endpoint");
        }
    }

    // create SINK routers and runtime if the cluster is not a connection starter
    private void startSinkComponents(Set<ClusterDescriptor> connectionEndpointsToLocalCluster,
                                     Map<ReplicationSession, LogReplicationSinkServerRouter> sessionToSinkServe) {
        log.info("Starting Sink (receiver) components");

        // start sink if the cluster is not a connection starter
        Set<String> allRemoteSourceClusterIds = new HashSet<>(topologyDescriptor.getRemoteSourceClusters().keySet());
        connectionEndpointsToLocalCluster.stream().forEach(cluster -> allRemoteSourceClusterIds.remove(cluster.clusterId));

        // for testing purpose
        if (allRemoteSourceClusterIds.contains(localClusterDescriptor.clusterId)) {
            allRemoteSourceClusterIds.remove(localClusterDescriptor.clusterId);
        }

        if (!allRemoteSourceClusterIds.isEmpty()) {
            log.debug("starting sink and wait for connection from remote sources: {}", allRemoteSourceClusterIds);
            allRemoteSourceClusterIds.stream().forEach(clusterId -> {
                log.info("remoteClusterIdToReplicationSession : {}", remoteClusterIdToReplicationSession);
                for (ReplicationSession session : remoteClusterIdToReplicationSession.get(clusterId)) {
                    // create incoming session so that the transport layer is able to route the incoming msgs to the correct router.
                    ReplicationSession incomingSession = new ReplicationSession(
                            localClusterDescriptor.getClusterId(), session.getRemoteClusterId(), session.getSubscriber());
                    sessionToSinkServe.put(incomingSession,
                            replicationManager.getOrCreateSinkRouter(topologyDescriptor.getRemoteSinkClusters().get(clusterId),
                            session, serverMap,false)
                    );
                }
            });
        } else {
            log.debug("No remote cluster found for which this cluster is a connection endpoint");
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
                    topologyDescriptor = new TopologyDescriptor(topologyMessage, this.clusterManagerAdapter);
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
        if (localClusterDescriptor != null && localClusterDescriptor.getRole() == ClusterRole.SOURCE && isLeader.get()) {
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
     * Update Topology Config Id on the Sink components, including SinkManager
     * so messages are filtered on the most up to date topologyConfigId
     */
    private void updateTopologyConfigIdOnSink(long configId) {
        if (interClusterServerNode != null) {
            interClusterServerNode.updateTopologyConfigId(configId);
        }
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
        if (localClusterDescriptor != null && localClusterDescriptor.getRole() == ClusterRole.SINK) {
            interClusterServerNode.setLeadership(false);
        }
        recordLockRelease();
    }

    /**
     * Process Topology Config Change:
     * - Higher config id
     * - Potential cluster role change
     * <p>
     * Cluster change from Source to Sink is a two step process, we first
     * confirm that
     * we are ready to do the cluster role change, so by the time we receive cluster change
     * notification, nothing needs to be done, other than stop.
     *
     * @param newTopology new discovered topology
     */
    public void onClusterRoleChange(TopologyDescriptor newTopology) {

        log.debug("OnClusterRoleChange, topology={}", newTopology);

        // Stop ongoing replication, stopLogReplication() checks leadership and role as SOURCE
        // We do not update topology until we successfully stop log replication
        if (!topologyDescriptor.getRemoteSinkClusters().isEmpty()) {
            stopLogReplication();
            logReplicationEventListener.stop();
        }
        if (newTopology.getRemoteSourceClusters().isEmpty()) {
            // Stop the replication server, present when the cluster is a connectionEndpoint.
            if (interClusterServerNode != null) {
                interClusterServerNode.disable();
            }
            replicationManager.clearSessionToSinkRouterMap();
        } else {
            updateTopologyConfigIdOnSink(newTopology.getTopologyConfigId());
        }

        if (isLeader.get()) {
            // Reset the Replication Status on Source and Sink only on the
            // leader node.  Consider the case of async configuration changes,
            // non-lead nodes could overwrite the replication status if it
            // has already completed by the lead node
            resetReplicationStatusTableWithRetry();
        }

        // Clear existing Metadata Managers.
        remoteSessionToMetadataManagerMap.clear();

        // Update topology, cluster, and node configs
        log.debug("Update existing topologyConfigId {}, cluster id={}, " +
            "role={} with the new topology",
            topologyDescriptor.getTopologyConfigId(),
            localClusterDescriptor.getClusterId(),
            localClusterDescriptor.getRole());
        updateLocalTopologyAndContext(newTopology);

        // Update with the new roles
        performRoleBasedSetups(newTopology);

        // On Topology Config Change, only if this node is the leader take action
        if (isLeader.get()) {
            onLeadershipAcquire();
        }
    }

    /**
     * Process a topology change as provided by the Cluster Manager
     * <p>
     * Note: We are assuming that topology configId change implies a role change.
     * The number of sink clusters change would not bump config id.
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

        log.debug("Received topology change, topology={} {}", event.getTopologyConfig(), event.getTopologyConfig());

        TopologyDescriptor discoveredTopology = new TopologyDescriptor(event.getTopologyConfig(), clusterManagerAdapter);

        boolean isValid;
        try {
            isValid = processDiscoveredTopology(discoveredTopology,
                localClusterDescriptor == null);
        } catch (Throwable t) {
            log.error("Exception when processing the discovered topology", t);
            stopLogReplication();
            return;
        }

        if (isValid) {
            if (clusterRoleChanged(discoveredTopology)) {
                onClusterRoleChange(discoveredTopology);
            } else {
                onRemoteClusterAddRemove(discoveredTopology);
            }
        } else {
            // Stop Log Replication in case this node was previously SOURCE but no longer belongs to the Topology
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
     * Process a topology change where a Sink cluster has been added or removed from the topology.
     *
     * @param discoveredTopology new discovered topology
     */
    private void onRemoteClusterAddRemove(TopologyDescriptor discoveredTopology) {
        log.debug("Remote Clusters have been added or removed");

        // We only need to process new sinks, so find if any remote sink has been added/removed
        Set<String> receivedRemoteSinks = discoveredTopology.getRemoteSinkClusters().keySet();
        Set<String> currentRemoteSinks = topologyDescriptor.getRemoteSinkClusters().keySet();
        Set<String> remoteClustersToRemove = Sets.difference(currentRemoteSinks, receivedRemoteSinks);
        Set<String> remoteSinkClustersToAdd = Sets.difference(receivedRemoteSinks, currentRemoteSinks);

        Set<String> receivedRemoteSource = discoveredTopology.getRemoteSourceClusters().keySet();
        Set<String> currentRemoteSource = topologyDescriptor.getRemoteSourceClusters().keySet();
        Sets.difference(currentRemoteSource, receivedRemoteSource).stream().forEach(clusterId -> remoteClustersToRemove.add(clusterId));
        Set<String> remoteSourceClustersToAdd = Sets.difference(receivedRemoteSource, currentRemoteSource);

        log.debug("remote clusters removed {}", remoteClustersToRemove);
        log.debug("remote sink to add {} and remote source to add {}", remoteSinkClustersToAdd, remoteSourceClustersToAdd);

        if ((!remoteClustersToRemove.isEmpty() || !remoteSinkClustersToAdd.isEmpty() || !remoteSourceClustersToAdd.isEmpty()) &&
                replicationManager != null && isLeader.get()) {

            Set<String> intersection = Sets.intersection(currentRemoteSinks, receivedRemoteSinks);

            // remove remote sinks
            for (String remoteClusterId : remoteClustersToRemove) {
                for(ReplicationSession session : remoteClusterIdToReplicationSession.get(remoteClusterId)) {
                    removeClusterInfoFromStatusTable(session);
                }
                remoteClusterIdToReplicationSession.remove(remoteClusterId);
            }
            createMetadataManagers(remoteSinkClustersToAdd, discoveredTopology);
            createMetadataManagers(remoteSourceClustersToAdd, discoveredTopology);

            initReplicationStatusForRemoteClusters(remoteSinkClustersToAdd,true);
            initReplicationStatusForRemoteClusters(remoteSourceClustersToAdd,false);

            replicationContext.setTopology(discoveredTopology);
            replicationManager.processRemoteClusterChange(discoveredTopology, remoteSinkClustersToAdd, remoteSourceClustersToAdd,
                    remoteClustersToRemove, intersection, fetchConnectionEndpoints(), remoteClusterIdToReplicationSession,
                    serverMap);
        } else {
            // Update the topology config id on the Sink components
            updateTopologyConfigIdOnSink(discoveredTopology.getTopologyConfigId());
        }

        // Update Topology Config Id on MetadataManagers (contains persisted
        // metadata tables)
        remoteSessionToMetadataManagerMap.values().forEach(metadataManager -> metadataManager.setupTopologyConfigId(
            discoveredTopology.getTopologyConfigId()));

        updateLocalTopologyAndContext(discoveredTopology);
        log.debug("Persisted new topologyConfigId {}, cluster id={}, role={}", topologyDescriptor.getTopologyConfigId(),
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
            log.info("Node[{}/{}] belongs to cluster, descriptor={}, topology={}",
                localEndpoint, localNodeId, localClusterDescriptor, topology);
            if (!bootstrapComplete) {
                log.info("Bootstrap the Log Replication Service");
                performRoleBasedSetups(topology);
                registerToLogReplicationLock();
                bootstrapComplete = true;
            }
            return true;
        }

        // If a cluster descriptor is not found, this node does not belong to any cluster in the topology
        // wait for updates to the topology config to start, if this cluster ever becomes part of the topology
        log.warn("Node[{}/{}] does not belong to any cluster provided by the discovery service, topology={}",
                localEndpoint, localNodeId, topology);
        return false;
    }

    private void updateLocalTopologyAndContext(TopologyDescriptor newConfig) {
        // Update local topology descriptor
        topologyDescriptor = newConfig;

        // Update local cluster descriptor
        localClusterDescriptor = topologyDescriptor.getClusterDescriptor(localNodeId);

        // Update local node descriptor
        localNodeDescriptor = localClusterDescriptor.getNode(localNodeId);

        // replicationContext is null when a cluster was never the source for any session, but had a role change and
        // became the source for the first time.
        if (replicationContext != null) {
            replicationContext.setTopology(newConfig);
        }
    }

    /**
     * Enforce a snapshot sync for the sink cluster in the event if the
     * current node is an source leader node
     */
    private void processEnforceSnapshotSync(DiscoveryServiceEvent event) {

        // A switchover could have happened after the SOURCE received the
        // command and wrote it to the event table.  So check the cluster role
        // here again.
        if (localClusterDescriptor.getRole() == ClusterRole.SINK) {
            log.warn("The current role is STANDBY.  Ignoring the forced snapshot sync event");
            return;
        }
        if (replicationManager == null || !isLeader.get()) {
            log.warn("The current node is not the leader, will skip doing the forced snapshot sync with id {}",
                event.getEventId());
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
     * Source Cluster - Read the shared metadata table to find the status of any ongoing snapshot or log entry sync
     * and return a completion percentage.
     * <p>
     * Sink Cluster - Read the shared metadata table and find if data is consistent(set to false if snapshot sync is
     * in the apply phase)
     */
    @Override
    public Map<String, LogReplicationMetadata.ReplicationStatusVal> queryReplicationStatus() {
        Map<String, LogReplicationMetadata.ReplicationStatusVal> clientToReplicationStatusMap = new HashMap<>();

        if (localClusterDescriptor == null) {
            log.warn("Cluster configuration has not been pushed to current LR node.");
            return clientToReplicationStatusMap;
        }

        if (localClusterDescriptor.getRole() != ClusterRole.SOURCE &&
            localClusterDescriptor.getRole() != ClusterRole.SINK) {
            log.error("Received Replication Status Query in Incorrect Role {}.", localClusterDescriptor.getRole());
            return clientToReplicationStatusMap;
        }

        // Note: MetadataManager is currently instantiated per remote session.  In a subsequent PR, change to share a
        // single instance for all remote sessions will be added.  So for now, get all replication statuses using any
        // 1 metadata manager.
        if (remoteSessionToMetadataManagerMap.values().iterator().hasNext()) {
            return remoteSessionToMetadataManagerMap.values().iterator().next().getReplicationStatus();
        }
        return clientToReplicationStatusMap;
    }

    @Override
    public UUID forceSnapshotSync(String clusterId) throws LogReplicationDiscoveryServiceException {
        if (localClusterDescriptor.getRole() == ClusterRole.SINK) {
            String errorStr = "The forceSnapshotSync command is not supported on sink cluster.";
            log.error(errorStr);
            throw new LogReplicationDiscoveryServiceException(errorStr);
        }

        UUID forceSyncId = UUID.randomUUID();
        log.info("Received forceSnapshotSync command for sink cluster {}, forced sync id {}",
                clusterId, forceSyncId);

        // Write a force sync event to the logReplicationEventTable
        ReplicationEventKey key = ReplicationEventKey.newBuilder().setKey(System.currentTimeMillis() + " " + clusterId).build();
        ReplicationEvent event = ReplicationEvent.newBuilder()
                .setClusterId(clusterId)
                .setEventId(forceSyncId.toString())
                .setType(ReplicationEvent.ReplicationEventType.FORCE_SNAPSHOT_SYNC)
                .build();

        // TODO: Define how forced snapshot sync should work.  Can it be requested for a given client or for the
        //  whole cluster?
        // For now, get the only supported(default) replication model and client for this cluster and trigger the
        // operation on it.
        remoteSessionToMetadataManagerMap.get(ReplicationSession.getDefaultReplicationSessionForCluster(clusterId, localClusterDescriptor.getClusterId()))
            .updateLogReplicationEventTable(key, event);
        return forceSyncId;
    }

    @Override
    public ClusterRole getLocalClusterRoleType() {
        return localClusterDescriptor.getRole();
    }

    @Override
    public ClusterDescriptor getLocalCluster() {
        return localClusterDescriptor;
    }

    public String getLocalNodeId() {
        return localNodeId;
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

        remoteSessionToMetadataManagerMap.values().forEach(metadataManager -> metadataManager.shutdown());

        if (interClusterServerNode != null) {
            interClusterServerNode.close();
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

    private void initReplicationStatusForRemoteClusters(Set<String> remoteClusterIds, boolean isSource) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    if (remoteClusterIds != null) {
                        remoteClusterIds.stream().forEach(remoteClusterId -> {
                            if (remoteClusterIdToReplicationSession.get(remoteClusterId) != null) {
                                remoteClusterIdToReplicationSession.get(remoteClusterId).forEach(session -> {
                                    remoteSessionToMetadataManagerMap.get(session).initReplicationStatus(session, isSource);
                                });
                            }
                        });
                        log.debug("Replication Status for new remote added successfully.");
                    }
                } catch (TransactionAbortedException tae) {
                    log.error("Error while attempting to update Replication Status for new remote", tae);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to add Replication Status for new remote.", e);
            throw new UnrecoverableCorfuInterruptedError(e);
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
            log.error("setupLocalNodeId failed, because nodeId file path is " +
                "missing!");
            DefaultClusterConfig defaultClusterConfig = new DefaultClusterConfig();

            // For testing purpose, it uses the default host to assign node id
            if (getLocalHost().equals(defaultClusterConfig.getDefaultHost())) {
                localNodeId = defaultClusterConfig.getDefaultNodeId(localEndpoint);
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

    private void removeClusterInfoFromStatusTable(ReplicationSession session) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    remoteSessionToMetadataManagerMap.get(session).removeFromStatusTable(session.getRemoteClusterId());
                } catch (TransactionAbortedException tae) {
                    log.error("Error while attempting to remove clusterInfo from LR status tables", tae);
                    throw new RetryNeededException();
                }

                log.debug("removeClusterInfoFromStatusTable succeeds, removed clusterID {}", session.getRemoteClusterId());

                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to removeClusterInfoFromStatusTable", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    private void resetReplicationStatusTableWithRetry() {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    // Note: MetadataManager is currently instantiated per remote session.  In a subsequent PR, change
                    // to share a single instance for all remote sessions will be added.  So for now, reset the table
                    // using any 1 metadata manager.
                    if (remoteSessionToMetadataManagerMap.values().iterator().hasNext()) {
                        remoteSessionToMetadataManagerMap.values().iterator().next().resetReplicationStatus();
                    }
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
