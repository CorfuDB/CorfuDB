package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Timestamp;
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
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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

    private final AtomicLong numLockAcquisitions = new AtomicLong(0);

    private CorfuInterClusterReplicationServerNode interClusterServerNode;

    private final ServerContext serverContext;

    private String localCorfuEndpoint;

    /**
     * Corfu Runtime to connect to the local Corfu Datastore.
     */
    private CorfuRuntime runtime;

    private boolean shouldRun = true;

    private LockClient lockClient;

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
     * Contains Log Replication plugin's configurations
     */
    private final LogReplicationPluginConfig pluginConfig;

    /**
     * Constructor Discovery Service
     *
     * @param serverContext current server's context
     */
    public CorfuReplicationDiscoveryService(@Nonnull ServerContext serverContext, @Nonnull CorfuRuntime runtime) {
        this.serverContext = serverContext;
        this.logReplicationLockId = serverContext.getNodeId();
        this.localEndpoint = serverContext.getLocalEndpoint();
        this.runtime = runtime;
        this.pluginConfig = new LogReplicationPluginConfig(serverContext.getPluginConfigFilePath());
        this.clusterManagerAdapter = getClusterManagerAdapter();
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
                interClusterServerNode = null;
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
     * Verify current node belongs to a cluster in the topology.
     *
     * @param topology discovered topology
     * @param update   indicates if the discovered topology should immediately be reflected as current (cached)
     */
    private boolean clusterPresentInTopology(TopologyDescriptor topology, boolean update) {
        if (topology.getLocalClusterDescriptor() != null && topology.getLocalNodeDescriptor() != null) {
            if (update) {
                topologyDescriptor = topology;
                int port = topology.getLocalClusterDescriptor().getCorfuPort();
                localCorfuEndpoint = CorfuSaasEndpointProvider.getCorfuSaasEndpoint()
                        .orElseGet(() -> getCorfuEndpoint(getLocalHost(), port));
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
     * Depending on the role of the cluster to which this leader node belongs (Source, Sink or both), it will create
     * respective components, and trigger connection if local cluster is the connection starter to any remote cluster,
     * else we wait to receive a connection request.
     * Replication starts after connection to/from a remote cluster is successful.
     */
    private void onLeadershipAcquire() {
        if (!isSource(topologyDescriptor) && !isSink(topologyDescriptor)) {
            log.error("Log Replication not started on this cluster. Remote source/sink not found");
            return;
        }

        sessionManager.notifyLeadershipChange();
        performReplicationSetup(topologyDescriptor);
        // record metrics about the lock acquired.
        lockAcquireSample = recordLockAcquire();
        processCountOnLockAcquire();
    }

    private void performReplicationSetup(TopologyDescriptor topology) {
        sessionManager.refresh(topology);

        // Update the topology
        // Note: Topology will be different only if we reached here due to a topology change
        topologyDescriptor = topology;

        // Setup the connection components if this node is the leader
        if (sessionManager.getReplicationContext().getIsLeader().get()) {
            setupConnectionComponents();
        }

        // If invoked on a topology change and no longer a Source, stop the listeners
        if (!sessionManager.getReplicationContext().getIsLeader().get() && !isSource(topologyDescriptor)) {
            if (logReplicationEventListener != null) {
                logReplicationEventListener.stop();
            }
            sessionManager.stopClientConfigListener();
        }

        // If the current cluster is a Source and a leader, start the listeners
        if (isSource(topologyDescriptor) && sessionManager.getReplicationContext().getIsLeader().get()) {
            if (logReplicationEventListener == null) {
                logReplicationEventListener = new LogReplicationEventListener(this, runtime);
            }
            logReplicationEventListener.start();
            sessionManager.startClientConfigListener();
        }
    }

    private void setupConnectionComponents() {
        setupConnectionReceivingComponents();
        sessionManager.connectToRemoteClusters();
    }

    /**
     * Create CorfuInterClusterReplicationServerNode when the cluster is a connection endpoint.
     */
    private void setupConnectionReceivingComponents() {
        if (!sessionManager.isConnectionReceiver()) {
            if (interClusterServerNode != null) {
                // Stop the replication server.
                // There may be a topology change where the remote cluster that would connect to the local cluster was
                // removed from the topology.
                interClusterServerNode.close();
                interClusterServerNode = null;
            }
            return;
        }

        if (interClusterServerNode == null) {
            interClusterServerNode = new CorfuInterClusterReplicationServerNode(serverContext,
                    sessionManager.getRouter());
        } else {
            //Start the server again as it was previously shutdown due to topology change.(start operation is idempotent)
            interClusterServerNode.startServer();
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
    private void stopLogReplication() {
        // Session manager is created only on bootstrap completion.  So check for bootstrap completion to avoid NPE
        // while accessing session manager
        if (bootstrapComplete && sessionManager.getReplicationContext().getIsLeader().get()) {
            log.info("Stopping log replication.");
            if(logReplicationEventListener != null) {
                logReplicationEventListener.stop();
            }
            sessionManager.stopClientConfigListener();
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
     * Process lock release event
     * <p>
     * Set leadership metadata and stop log replication in the event of leadership loss
     */
    public void processLockRelease() {
        log.debug("Lock released");
        sessionManager.getReplicationContext().setIsLeader(false);
        sessionManager.notifyLeadershipChange();
        stopLogReplication();
        recordLockRelease();
    }

    private boolean isSource(TopologyDescriptor descriptor) {
        return !descriptor.getRemoteSinkClusters().isEmpty();
    }

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
            isValid = processDiscoveredTopology(discoveredTopology, topologyDescriptor.getLocalClusterDescriptor() == null);
        } catch (Throwable t) {
            log.error("Exception when processing the discovered topology", t);
            stopLogReplication();
            return;
        }

        if (isValid) {
            onTopologyChange(discoveredTopology);
        } else {
            // Stop Log Replication in case this node was previously SOURCE but no longer belongs to the Topology
            stopLogReplication();
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

        performReplicationSetup(newTopology);

        log.debug("Persisted new topologyConfigId {}, cluster id={}", topologyDescriptor.getTopologyConfigId(),
            topologyDescriptor.getLocalClusterDescriptor().getClusterId());
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
                localEndpoint, localNodeId, topology.getLocalClusterDescriptor(), topology);
            if (!bootstrapComplete) {
                log.info("Bootstrap the Log Replication Service");
                sessionManager = new SessionManager(topologyDescriptor, runtime, serverContext, localCorfuEndpoint,
                        pluginConfig);
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

    /**
     * Enforce a snapshot sync for the sink cluster in the event if the
     * current node is an source leader node
     */
    private void processEnforceSnapshotSync(DiscoveryServiceEvent event) {

        // A switchover could have happened after the SOURCE received the
        // command and wrote it to the event table.  So check the cluster role
        // here again.
        if (!topologyDescriptor.getRemoteSinkClusters().containsKey(event.getSession().getSinkClusterId())) {
            log.warn("The local cluster does not have a SINK with ID {}.  Ignoring the forced snapshot sync event",
                    event.getSession().getSinkClusterId());
            return;
        }
        if (!sessionManager.getReplicationContext().getIsLeader().get()) {
            log.warn("Node is not the leader - skipping forced snapshot sync, id={}", event.getEventId());
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
    public Map<LogReplicationSession, ReplicationStatus> queryReplicationStatus() {
        Map<LogReplicationSession, ReplicationStatus> replicationStatusMap = new HashMap<>();

        if (topologyDescriptor.getLocalClusterDescriptor() == null) {
            log.warn("Cluster configuration has not been pushed to current LR node.");
            return replicationStatusMap;
        }

        if (sessionManager == null) {
            log.warn("Log Replication Service has not been bootstrapped yet.");
            return replicationStatusMap;
        }

        if (!isSource(topologyDescriptor) && !isSink(topologyDescriptor)) {
            log.error("Received Replication Status Query in Incorrect Role, cluster is neither SOURCE/SINK");
            return replicationStatusMap;
        }

        return sessionManager.getReplicationStatus();
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

        if (interClusterServerNode != null) {
            interClusterServerNode.close();
            interClusterServerNode = null;
        }

        if (sessionManager != null) {
            sessionManager.shutdown();
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

    @Override
    public Set<LogReplicationSession> getOutgoingSessions() {
        if (sessionManager != null) {
            return sessionManager.getOutgoingSessions();
        }
        return new HashSet<>();
    }

    @Override
    public Set<LogReplicationSession> getIncomingSessions() {
        if (sessionManager != null) {
            return sessionManager.getIncomingSessions();
        }
        return new HashSet<>();
    }
}
