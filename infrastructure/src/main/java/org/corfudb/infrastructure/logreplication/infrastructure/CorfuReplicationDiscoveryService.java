package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.LogReplicationServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.CorfuReplicationClusterManagerAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationStreamNameTableManager;
import org.corfudb.infrastructure.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.ClusterRole;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.utils.lock.LockClient;
import org.corfudb.utils.lock.LockListener;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

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

    private static final int CLUSTER_MANAGER_CONNECT_RETRIES = 10;
    private static final int CONNECT_SLEEP_DURATION = 5000;

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

    private boolean isLeader;

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
        this.isLeader = false;
    }

    public void run() {
        try {
            startDiscovery();

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
        } catch (LogReplicationDiscoveryServiceException e) {
            log.error("Exceptionally terminate Log Replication Discovery Service", e);
            serverCallback.completeExceptionally(e);
        } catch (Exception e) {
            log.error("Unhandled exception caught during log replication service discovery. Retry,", e);
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

        connectAndQueryTopology();

        // Health check - confirm this node belongs to a cluster in the topology
        if (!clusterPresentInTopology(topologyDescriptor)) {
            // If a cluster descriptor is not found, this node does not belong to any cluster in the topology
            String message = String.format("Node[%s] does not belong to any Cluster provided by the discovery service, topology=%s",
                    localEndpoint, topologyDescriptor);
            log.warn(message);
            throw new LogReplicationDiscoveryServiceException(message);
        }

        log.info("Node[{}] belongs to cluster, descriptor={}", localEndpoint, localClusterDescriptor);
        buildLogReplicationContext();

        // Unblock server initialization & register to Log Replication Lock, to attempt lock / leadership acquisition
        serverCallback.complete(interClusterReplicationService);

        registerToLogReplicationLock();
    }

    private void connectAndQueryTopology() throws LogReplicationDiscoveryServiceException {
        for (int retry=0; retry < CLUSTER_MANAGER_CONNECT_RETRIES; retry++) {
            try {
                // Connect to Cluster Manager and Retrieve Topology Info
                log.info("Connecting to Cluster Manager adapter...");
                clusterManagerAdapter.connect(this);
                log.info("Fetch topology from Cluster Manager...");
                TopologyConfigurationMsg topologyMessage = clusterManagerAdapter.fetchTopology();
                topologyDescriptor = new TopologyDescriptor(topologyMessage);
                return;
            } catch (Exception e) {
                String message = "Caught exception while fetching topology. Log Replication cannot start.";
                log.error(message, e);
                Sleep.sleepUninterruptibly(Duration.ofMillis(CONNECT_SLEEP_DURATION));
            }
        }

        throw new LogReplicationDiscoveryServiceException("Failed to connect and fetch topology. Abort.");
    }

    /**
     * Construct common log replication context.
     */
    private void buildLogReplicationContext() {
        // Through LogReplicationConfigAdapter retrieve system-specific configurations (including streams to replicate)
        LogReplicationConfig logReplicationConfig = getLogReplicationConfiguration(getCorfuRuntime());

        this.logReplicationMetadataManager = new LogReplicationMetadataManager(getCorfuRuntime(),
                topologyDescriptor.getTopologyConfigId(), localClusterDescriptor.getClusterId());

        LogReplicationServer logReplicationServerHandler = new LogReplicationServer(serverContext, logReplicationConfig,
                logReplicationMetadataManager, localCorfuEndpoint);
        logReplicationServerHandler.setActive(localClusterDescriptor.getRole().equals(ClusterRole.ACTIVE));

        this.interClusterReplicationService = new CorfuInterClusterReplicationServerNode(serverContext,
                logReplicationServerHandler, logReplicationConfig);

        // Pass server's channel context through the Log Replication Context, for shared objects between the server
        // and the client channel (specific requirements of the transport implementation)
        this.replicationContext = new LogReplicationContext(logReplicationConfig, topologyDescriptor,
                localCorfuEndpoint, interClusterReplicationService.getRouter().getServerAdapter().getChannelContext());
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
                    .build())
                    .parseConfigurationString(localCorfuEndpoint).connect();
        }

        return runtime;
    }

    /**
     * Verify current node belongs to a cluster in the topology.
     */
    private boolean clusterPresentInTopology(TopologyDescriptor topology) {
        localClusterDescriptor = topology.getClusterDescriptor(localEndpoint);
        if (localClusterDescriptor != null) {
            localNodeDescriptor = localClusterDescriptor.getNode(localEndpoint);
            localCorfuEndpoint = getCorfuEndpoint(getLocalHost(), localClusterDescriptor.getCorfuPort());
        }

        return localClusterDescriptor != null && localNodeDescriptor != null;
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
                    LockClient lock = new LockClient(logReplicationNodeId, getCorfuRuntime());
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
        switch (localNodeDescriptor.getRoleType()) {
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
                interClusterReplicationService.getLogReplicationServer().setLeadership(true);
                break;
            default:
                log.error("Log Replication not started on this cluster. Leader node {} belongs to cluster with {} role.",
                            localEndpoint, localNodeDescriptor.getRoleType());
                break;
        }
    }

    private void updateTopologyConfigId(boolean active) {
        // Required only on topology changes
        interClusterReplicationService.getLogReplicationServer().getSinkManager().updateTopologyConfigId(active, topologyDescriptor.getTopologyConfigId());

        log.debug("Persist new topologyConfigId {}, status={}", topologyDescriptor.getTopologyConfigId(),
                localNodeDescriptor.getRoleType());
    }

    /**
     * Stop Log Replication
     */
    private void stopLogReplication() {
        switch(localNodeDescriptor.getRoleType()) {
            case ACTIVE:
                log.info("This cluster has lost leadership. Stopping lof replication, according to role {}", localNodeDescriptor.getRoleType());
                replicationManager.stop();
                break;
            case STANDBY:
                log.info("This cluster has lost leadership. Stopping lof replication, according to role {}", localNodeDescriptor.getRoleType());
                // Signal Log Replication Server/Sink to stop receiving messages, leadership loss
                interClusterReplicationService.getLogReplicationServer().setLeadership(false);
                break;
            default:
                log.warn("Invalid role type {}. Failed to stop replication if any running.", localNodeDescriptor.getRoleType());
                break;
        }
    }

    /**
     * Process lock acquisition event
     */
    public void processLockAcquire() {
        log.debug("Process lock acquire event");
        isLeader = true;
        onLeadershipAcquire();
    }

    /**
     * Process lock release event
     *
     * Set leadership metadata and stop log replication in the event of leadership loss
     */
    public void processLockRelease() {
        log.debug("Process lock release event");
        isLeader = false;
        stopLogReplication();
    }

    /**
     * Process Cluster Role Change:
     *   - Standby to Active, or
     *   - Active to Standby
     *
     * @param newTopology new discovered topology
     */
    public void processClusterRoleChange(TopologyDescriptor newTopology) {
        stopLogReplication();
        //TODO pankti: read the configuration again and refresh the LogReplicationConfig object
        replicationManager.setTopology(newTopology);
        boolean activeCluster = localNodeDescriptor.getRoleType() == ClusterRole.ACTIVE;
        updateTopologyConfigId(activeCluster);

        // On Cluster Role Change, only if this node is the leader take action
        if (isLeader) {
            onLeadershipAcquire();
        }
    }

    /**
     * Process a topology change as provided by the Cluster Manager
     *
     * @param event discovery event
     */
    private void processTopologyChangeNotification(DiscoveryServiceEvent event) {
        // Stale notification, skip
        if (event.getTopologyConfig().getTopologyConfigID() < getReplicationManager().getTopology().getTopologyConfigId()) {
            log.debug("Stale Topology Change Notification, current={}, received={}", topologyDescriptor.getTopologyConfigId(),
                    event.getTopologyConfig());
            return;
        }

        TopologyDescriptor newConfig = new TopologyDescriptor(event.getTopologyConfig());
        topologyDescriptor = newConfig;

        //On topology change notification, store latest topology and only process if current node is the leader
        if (isLeader) {
            if (newConfig.getTopologyConfigId() == getReplicationManager().getTopology().getTopologyConfigId()) {
                if (localNodeDescriptor.getRoleType() == ClusterRole.STANDBY) {
                    return;
                }

                // If the current node is active, compare with the current topologyConfig, see if there are additional or
                // removed standbys
                getReplicationManager().processStandbyChange(newConfig);
            } else {
                // TODO: Are we sure that when there is a topologyConfigId change it implies a role change
                //  and not a new standby added??
                processClusterRoleChange(newConfig);
            }
        }
    }

    /***
     * After an upgrade, the active site should perform a snapshot sync
     */
    private void processUpgrade(DiscoveryServiceEvent event) {
        if (localNodeDescriptor.getRoleType() == ClusterRole.ACTIVE) {
            // TODO pankti: is this correct?
            replicationManager.restart(event.getRemoteSiteInfo());
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
     * Query the current all replication stream log tail and remeber the max
     * and query each standbySite information according to the ackInformation decide all manay total
     * msg needs to send out.
     */
    @Override
    public void prepareSiteRoleChange() {
        replicationManager.prepareSiteRoleChange();
    }

    /**
     * Query all replicated stream log tails and calculate the number of messages to be sent.
     * If the max tail has changed, return 0%.
     */
    @Override
    public int queryReplicationStatus() {
        // TODO (maxi): address Nan's comments
        return replicationManager.queryReplicationStatus();
    }

    public void shutdown() {
        if (replicationManager != null) {
            replicationManager.stop();
        }

        if(clusterManagerAdapter != null) {
            clusterManagerAdapter.shutdown();
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
