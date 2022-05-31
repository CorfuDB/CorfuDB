package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.LongTaskTimer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType;
import org.corfudb.infrastructure.logreplication.infrastructure.utils.CorfuSaasEndpointProvider;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.CorfuReplicationClusterManagerAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEventInfoKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent.ReplicationEventType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.corfudb.common.util.URLUtils.getHostFromEndpointURL;
import static org.corfudb.common.util.URLUtils.getVersionFormattedEndpointURL;

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
    private final Map<String, LogReplicationMetadataManager>
        remoteClientToMetadataManagerMap = new HashMap<>();

    /**
     * Lock-related configuration parameters
     */
    private static final String LOCK_GROUP = "Log_Replication_Group";
    private static final String LOCK_NAME = "Log_Replication_Lock";

    /**
     * Responsible for creating and maintaining the replication sessions associated with each remote cluster and
     * replication model
     */
    @Getter
    private SessionManager sessionManager;

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
     * Current node's endpoint
     */
    @Getter
    @VisibleForTesting
    private final String localEndpoint;

    /**
     * Current node's id
     */
    @Getter
    @VisibleForTesting
    private String localNodeId;

    /**
     * Unique node identifier of lock
     */
    // Note: not to be confused with NodeDescriptor's NodeId, which is a unique
    // identifier for the node as reported by the Cluster/Topology Manager
    // This id is internal to LR and used for the lock acquisition
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

    /**
     * Corfu Runtime to connect to the local Corfu Datastore.
     */
    private CorfuRuntime runtime;

    private boolean shouldRun = true;

    @Getter
    private final AtomicBoolean isLeader;

    private LockClient lockClient;

    /**
     * Indicates that bootstrap has been completed. Bootstrap is done once it
     * is determined that this node belongs to a cluster in the topology
     * provided by ClusterManager and has the role of ACTIVE or STANDBY
     */
    private boolean bootstrapComplete = false;

    /**
     * This is the listener to the replication event table shared by the nodes in the cluster.
     * When a non-leader node is called to do the enforcedSnapshotSync, it will write the event to
     * the shared event-table and the leader node will be notified to do the work.
     */
    private LogReplicationEventListener logReplicationEventListener;

    /**
     * Contains Log Replication plugin's configurations
     */
    private final LogReplicationPluginConfig pluginConfig;

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

    /**
     * Create the Cluster Manager Adapter, i.e., the adapter to external provider of the topology.
     *
     * @return cluster manager adapter instance
     */
    private CorfuReplicationClusterManagerAdapter getClusterManagerAdapter() {

        File jar = new File(pluginConfig.getTopologyManagerAdapterJARPath());

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class adapter = Class.forName(pluginConfig.getTopologyManagerAdapterName(), true, child);
            return (CorfuReplicationClusterManagerAdapter) adapter.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            log.error("Fatal error: Failed to create serverAdapter", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    /**
     * Start Log Replication Discovery Service
     */
    public void start() {
        try {
            log.info("Start Log Replication Discovery Service");

            fetchTopology();
            processDiscoveredTopology(topologyDescriptor, true);

            while (shouldRun) {
                try {
                    DiscoveryServiceEvent event = eventQueue.take();
                    processEvent(event);
                } catch (Exception e) {
                    log.error("Caught an exception. Stop discovery service.", e);
                    shouldRun = false;
                    stopLogReplication(false);
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

     private void connectToClusterManager() {
        // The cluster manager orchestrates the Log Replication Service. If it is not available,
        // topology cannot be discovered and therefore LR cannot start, for this reason, connection
        // should be attempted indefinitely.
        try {
            clusterManagerAdapter.register(this);

            IRetry.build(IntervalRetry.class, () -> {
                try {
                    log.info("Connecting to cluster manager {}", clusterManagerAdapter.getClass().getSimpleName());
                    clusterManagerAdapter.start();
                } catch (Exception e) {
                    log.error("Error while attempting to connect to cluster manager. Retry.", e);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to connect to cluster manager.", e);
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
     * @param role
     */
    private void performRoleBasedSetup(ClusterRole role) {
        if (role != ClusterRole.ACTIVE && role != ClusterRole.STANDBY) {
            log.debug("Cluster role is {}.  Not performing role-based setup.", role);
            return;
        }

        // Through LogReplicationConfigAdapter retrieve system-specific configurations
        // such as streams to replicate and version
        LogReplicationConfig logReplicationConfig = getLogReplicationConfiguration(getCorfuRuntime());

        Set<String> remoteClusterIds = new HashSet<>();

        if (role == ClusterRole.ACTIVE) {
            remoteClusterIds.addAll(topologyDescriptor.getStandbyClusters().keySet());
            createMetadataManagers(remoteClusterIds);
            replicationContext = new LogReplicationContext(logReplicationConfig, topologyDescriptor, localCorfuEndpoint);
            logReplicationEventListener = new LogReplicationEventListener(this, getCorfuRuntime());
            logReplicationEventListener.start();
        } else {
            // Sink Cluster
            remoteClusterIds.addAll(topologyDescriptor.getActiveClusters().keySet());
            createMetadataManagers(remoteClusterIds);

            LogReplicationServer server = new LogReplicationServer(serverContext, localNodeId, logReplicationConfig,
                remoteClusterIds, localCorfuEndpoint, topologyDescriptor.getTopologyConfigId(),
                remoteClientToMetadataManagerMap);
            interClusterServerNode = new CorfuInterClusterReplicationServerNode(serverContext, server);
        }
    }

    private void createMetadataManagers(Set<String> remoteClusterIds) {
        for (String remoteClusterId : remoteClusterIds) {
            LogReplicationMetadataManager metadataManager = new LogReplicationMetadataManager(getCorfuRuntime(),
                topologyDescriptor.getTopologyConfigId(), remoteClusterId);
            remoteClientToMetadataManagerMap.put(remoteClusterId, metadataManager);
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
            return true;
        }
        return false;
    }

    /**
     * Retrieve local Corfu Endpoint
     */
    private String getCorfuEndpoint(String localHostAddress, int corfuPort) {
        return getVersionFormattedEndpointURL(localHostAddress, corfuPort);
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
                    lockClient = new LockClient(logReplicationLockId, runtime);
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
     * This method is only called on the leader node and is triggered on the start of log replication and on topology change
     * <p>
     * Depending on the role of the cluster to which this leader node belongs, it will start
     * as source (sender/producer) or sink (receiver).
     */
    private void onLeadershipAcquire() {
        switch (localClusterDescriptor.getRole()) {
            case ACTIVE:
                log.info("Start as Source (sender/replicator)");
                if (replicationManager == null) {
                    replicationManager = new CorfuReplicationManager(replicationContext,
                        localNodeDescriptor,
                        remoteClientToMetadataManagerMap,
                        serverContext.getPluginConfigFilePath(),
                            getCorfuRuntime(), replicationConfigManager);
                } else {
                    // Replication Context contains the topology which
                    // must be updated if it has changed
                    replicationManager.setContext(replicationContext);
                }
                replicationManager.start();

                // Set initial/default replication status for newly added Sink clusters
                initReplicationStatusForRemoteClusters(true);
                lockAcquireSample = recordLockAcquire(localClusterDescriptor.getRole());
                processCountOnLockAcquire(localClusterDescriptor.getRole());
                break;
            case STANDBY:
                log.info("Start as Sink (receiver)");

                // Sink Site : the LogReplicationServer (server handler) will
                // reset the LogReplicationSinkManager on acquiring leadership
                interClusterServerNode.setLeadership(true);
                lockAcquireSample = recordLockAcquire(localClusterDescriptor.getRole());

                // Set initial/default replication status for newly added Source clusters
                initReplicationStatusForRemoteClusters(false);
                processCountOnLockAcquire(localClusterDescriptor.getRole());
                break;
            default:
                log.error("Log Replication not started on this cluster. Leader node {} id {} belongs to cluster with {} role.",
                        localEndpoint, localNodeId, localClusterDescriptor.getRole());
                break;
        }
    }

    /**
     * Fetch current topology from cluster manager
     */
    private void fetchTopology() {

        connectToClusterManager();
        this.localNodeId = clusterManagerAdapter.getLocalNodeId();

        try {
            IRetry.build(ExponentialBackoffRetry.class, () -> {
                try {
                    log.info("Fetching topology from cluster manager...");
                    topologyDescriptor = clusterManagerAdapter.queryTopologyConfig(false);
                } catch (Exception e) {
                    log.error("Caught exception while fetching topology. Retry.", e);
                    throw new RetryNeededException();
                }
                return null;
            }).setOptions(x -> x.setMaxRetryThreshold(Duration.ofSeconds(FETCH_THRESHOLD))).run();
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        } catch (RetryExhaustedException ree) {
            log.warn("Failed to retrieve updated topology from cluster manager.");
        }
    }

    /**
     * Stop Log Replication
     */
    private void stopLogReplication(boolean lockReleased) {
        if (lockReleased || sessionManager.getReplicationContext().getIsLeader().get()) {
            log.info("Stopping log replication.");
            sessionManager.stopReplication();
        }
    }

    /**
     * Process lock acquisition event
     */
    public void processLockAcquire() {
        log.debug("Lock acquired");
        sessionManager.getReplicationContext().setIsLeader(true);
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
        if (localClusterDescriptor != null && localClusterDescriptor.getRole() == ClusterRole.STANDBY) {
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

        // Stop ongoing replication, stopLogReplication() checks leadership and active
        // We do not update topology until we successfully stop log replication
        if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE) {
            stopLogReplication();
            logReplicationEventListener.stop();
        } else if (localClusterDescriptor.getRole() == ClusterRole.STANDBY) {
            // Stop the replication server
            interClusterServerNode.disable();
        }

        if (isLeader.get()) {
            // Reset the Replication Status on Source and Sink only on the
            // leader node.  Consider the case of async configuration changes,
            // non-lead nodes could overwrite the replication status if it
            // has already completed by the lead node
            resetReplicationStatusTableWithRetry();
            // In the event of Standby -> Active we should add the default replication values
            if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE) {
                addDefaultReplicationStatus();
            }
        }

        // Clear existing Metadata Managers.
        remoteClientToMetadataManagerMap.clear();

        // Update topology, cluster, and node configs
        log.debug("Update existing topologyConfigId {}, cluster id={}, " +
            "role={} with the new topology",
            topologyDescriptor.getTopologyConfigId(),
            localClusterDescriptor.getClusterId(),
            localClusterDescriptor.getRole());
        updateLocalTopology(newTopology);

        // Update with the new roles
        performRoleBasedSetup(localClusterDescriptor.getRole());

    private boolean isSink(TopologyDescriptor descriptor) {
        return !descriptor.getRemoteSourceClusters().isEmpty();
    }


    /**
     * Process a topology change as provided by the Cluster Manager
     * <p>
     * Note: We are assuming that topology configId change implies a role change.
     * The number of sink clusters change would not bump config id.
     *
     * @param event discovery event
     */
    private void processTopologyChangeNotification(DiscoveryServiceEvent event) {
        if (event.getTopologyConfig().getTopologyConfigId() < topologyDescriptor.getTopologyConfigId()) {
            log.debug("Stale Topology Change Notification, current={}, received={}",
                    topologyDescriptor.getTopologyConfigId(), event.getTopologyConfig().getTopologyConfigId());
            return;
        }
        if (event.getTopologyConfig().equals(topologyDescriptor)) {
            log.debug("Duplicate topology received. Current topology {} received topology {}. Skipping the update.",
                    topologyDescriptor, event.getTopologyConfig());
            return;
        }

        log.debug("Received topology change, topology={}", event.getTopologyConfig());

        TopologyDescriptor discoveredTopology = event.getTopologyConfig();

        boolean isValid;
        try {
            isValid = processDiscoveredTopology(discoveredTopology,
                localClusterDescriptor == null);
        } catch (Throwable t) {
            log.error("Exception when processing the discovered topology", t);
            stopLogReplication(false);
            return;
        }

        if (isValid) {
            onTopologyChange(discoveredTopology);
        } else {
            // Stop Log Replication in case this node was previously SOURCE but no longer belongs to the Topology
            stopLogReplication(false);
        }
    }

    /**
     * Process a topology change where a remote cluster has been added or removed from the topology.
     * (A role change is also treated as same)
     *
     * @param newTopology the new discovered topology
     */
    private void onTopologyChange(TopologyDescriptor newTopology) {
        log.info("A role change or a remote cluster may have been added or removed");

    /**
     * Process a topology change where a Sink cluster has been added or removed from the topology.
     *
     * @param discoveredTopology new discovered topology
     */
    private void onStandbyClusterAddRemove(TopologyDescriptor discoveredTopology) {
        log.debug("Sink Cluster has been added or removed");

        // We only need to process new sinks if the local cluster role is SOURCE
        if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE &&
            replicationManager != null && isLeader.get()) {
            Set<String> receivedSinks = discoveredTopology.getStandbyClusters().keySet();
            Set<String> currentSinks = topologyDescriptor.getStandbyClusters().keySet();

            Set<String> intersection = Sets.intersection(currentSinks, receivedSinks);

            Set<String> sinksToRemove = Sets.difference(currentSinks, receivedSinks);

            Set<String> sinksToAdd = Sets.difference(receivedSinks, currentSinks);

            for (String remoteClusterId : sinksToRemove) {
                remoteClientToMetadataManagerMap.remove(remoteClusterId);
            }
            createMetadataManagers(sinksToAdd);
            initReplicationStatusForRemoteClusters(true);
            replicationContext.setTopology(discoveredTopology);
            replicationManager.processStandbyChange(discoveredTopology, sinksToAdd, sinksToRemove, intersection);
        } else {
            // Update the topology config id on the Sink components
            updateTopologyConfigIdOnSink(discoveredTopology.getTopologyConfigId());
        }

        // Update Topology Config Id on MetadataManagers (contains persisted
        // metadata tables)
        remoteClientToMetadataManagerMap.values().forEach(metadataManager -> metadataManager.setupTopologyConfigId(
            discoveredTopology.getTopologyConfigId()));

        updateLocalTopology(discoveredTopology);
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
    private boolean processDiscoveredTopology(@Nonnull TopologyDescriptor topology, boolean update) {
        // Health check - confirm this node belongs to a cluster in the topology
        if (topology != null && clusterPresentInTopology(topology, update)) {
            log.info("Node[{}/{}] belongs to cluster, descriptor={}, topology={}",
                localEndpoint, localNodeId, localClusterDescriptor, topology);
            if (!bootstrapComplete) {
                log.info("Bootstrap the Log Replication Service");
                performRoleBasedSetup(topology.getClusterDescriptor(localNodeId).getRole());
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

    private void updateLocalTopology(TopologyDescriptor newConfig) {
        // Update local topology descriptor
        topologyDescriptor = newConfig;

        // Update local cluster descriptor
        localClusterDescriptor = topologyDescriptor.getClusterDescriptor(localNodeId);

        // Update local node descriptor
        localNodeDescriptor = localClusterDescriptor.getNode(localNodeId);
    }

    /**
     * Enforce a snapshot sync for the standby cluster in the event if the
     * current node is an active leader node
     */
    private void processEnforceSnapshotSync(DiscoveryServiceEvent event) {

        // A switchover could have happened after the ACTIVE received the
        // command and wrote it to the event table.  So check the cluster role
        // here again.
        if (localClusterDescriptor.getRole() == ClusterRole.STANDBY) {
            log.warn("The current role is STANDBY.  Ignoring the forced " +
                "snapshot sync event");
            return;
        }
        if (replicationManager == null || !isLeader.get()) {
            log.warn("The current node is not the leader, will skip doing the " +
                    "forced snapshot sync with id {}", event.getEventId());
            return;
        }

        sessionManager.enforceSnapshotSync(event);
    }

    public synchronized void input(DiscoveryServiceEvent event) {
        eventQueue.add(event);
        notifyAll();
    }

    public void updateTopology(TopologyDescriptor topologyConfig) {
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

        if (localClusterDescriptor.getRole() != ClusterRole.ACTIVE &&
            localClusterDescriptor.getRole() != ClusterRole.STANDBY) {
            log.error("Received Replication Status Query in Incorrect Role {}.", localClusterDescriptor.getRole());
            return clientToReplicationStatusMap;
        }

        // Note: MetadataManager is currently instantiated per remote cluster.  In a subsequent PR, change to share a
        // single instance for all remote clusters will be added.  So for now, get all replication statuses using any
        // 1 metadata manager.
        if (remoteClientToMetadataManagerMap.values().iterator().hasNext()) {
            return remoteClientToMetadataManagerMap.values().iterator().next().getReplicationStatus();
        }
        return clientToReplicationStatusMap;
    }


    /**
     * Called by clients when needed to enforce a snapshot sync for a session.
     *
     * @param session against which the local cluster has to enforce a snapshot sync
     * @return event ID
     * @throws LogReplicationDiscoveryServiceException
     */
    public UUID forceSnapshotSync(LogReplicationSession session) throws LogReplicationDiscoveryServiceException {
        if (!topologyDescriptor.getLocalClusterDescriptor().getClusterId().equals(session.getSourceClusterId())) {
            String errorStr = "The session with sourceClusterID " + session.getSourceClusterId()+" and sinkClusterId " +
                    session.getSinkClusterId() +" does not belong to the local cluster: " +
                    topologyDescriptor.getLocalClusterDescriptor().getClusterId();
            log.error(errorStr);
            throw new LogReplicationDiscoveryServiceException(errorStr);

        } else if(!topologyDescriptor.getRemoteSinkClusters().containsKey(session.getSinkClusterId())) {
            String errorStr = "the localCluster " + topologyDescriptor.getLocalClusterDescriptor().getClusterId()+
                    " does not have a SINK with clusterId " + session.getSinkClusterId();
            log.error(errorStr);
            throw new LogReplicationDiscoveryServiceException(errorStr);
        }

        UUID forceSyncId = UUID.randomUUID();
        log.info("Received forced snapshot sync request for session {}, sync_id={}", session, forceSyncId);

        // Write a force sync event to the logReplicationEventTable
        ReplicationEventInfoKey key = ReplicationEventInfoKey.newBuilder()
            .setSession(session)
            .build();

        ReplicationEvent event = ReplicationEvent.newBuilder()
                .setEventId(forceSyncId.toString())
                .setType(ReplicationEventType.FORCE_SNAPSHOT_SYNC)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .build();
        remoteClientToMetadataManagerMap.get(clusterId).updateLogReplicationEventTable(key,
            event);
        return forceSyncId;
    }

        sessionManager.getMetadataManager().addEvent(key, event);
        return forceSyncId;
    }

    public void shutdown() {
        if (logReplicationEventListener != null) {
            logReplicationEventListener.stop();
        }

        if (clusterManagerAdapter != null) {
            clusterManagerAdapter.shutdown();
        }

        if (lockClient != null) {
            lockClient.shutdown();
        }

        remoteClientToMetadataManagerMap.values().forEach(metadataManager -> metadataManager.shutdown());

        if (interClusterServerNode != null) {
            interClusterServerNode.close();
        }

        serverContext.close();
    }

    /**
     * Return host for local node
     *
     * @return current node's IP
     */
    private String getLocalHost() {
        return getHostFromEndpointURL(serverContext.getLocalEndpoint());
    }

    private Optional<LongTaskTimer.Sample> recordLockAcquire() {
        return MeterRegistryProvider.getInstance()
                .map(registry -> registry.more()
                        .longTaskTimer("logreplication.lock.duration.nanoseconds")
                        .start());
    }

    // TODO[V2]: this looks incomplete.
    private void processCountOnLockAcquire() {
        MeterRegistryProvider.getInstance()
                .ifPresent(registry -> numLockAcquisitions.getAndIncrement());
    }

    private void recordLockRelease() {
        lockAcquireSample.ifPresent(LongTaskTimer.Sample::stop);
    }

    private void initReplicationStatusForRemoteClusters(boolean isSource) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    remoteClientToMetadataManagerMap.values().forEach(
                        metadataManager -> metadataManager.initReplicationStatus(isSource));
                } catch (TransactionAbortedException tae) {
                    log.error("Error while attempting to update Replication Status for new remote", tae);
                    throw new RetryNeededException();
                }
                log.debug("Replication Status for new remote added successfully.");
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
        return new HashSet<>();
    }

    private void removeClusterInfoFromStatusTable(String clusterId) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    remoteClientToMetadataManagerMap.get(clusterId).removeFromStatusTable(clusterId);
                } catch (TransactionAbortedException tae) {
                    log.error("Error while attempting to remove clusterInfo from LR status tables", tae);
                    throw new RetryNeededException();
                }

                log.debug("removeClusterInfoFromStatusTable succeeds, removed clusterID {}", clusterId);

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
                    // Note: MetadataManager is currently instantiated per remote cluster.  In a subsequent PR, change
                    // to share a single instance for all remote clusters will be added.  So for now, reset the table
                    // using any 1 metadata manager.
                    if (remoteClientToMetadataManagerMap.values().iterator().hasNext()) {
                        remoteClientToMetadataManagerMap.values().iterator().next().resetReplicationStatus();
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
        return new HashSet<>();
    }
}
