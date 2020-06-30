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
 *
 * - Topology discovery (active and standby's)
 * - Lock Acquisition (leader election)
 * - Log Replication Configuration (streams to replicate)
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
    private CorfuReplicationClusterManagerAdapter clusterManagerAdapter;

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
    private final String localEndpoint;

    /**
     * Local host
     */
    private final String localHost;

    /**
     * Current node information
     */
    private NodeDescriptor localNodeDescriptor;

    /**
     * Current node's leader flag
     */
    private boolean isLeader;

    /**
     * Unique node identifier
     */
    private final UUID nodeId;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<DiscoveryServiceEvent> eventQueue = new LinkedBlockingQueue<>();

    private CompletableFuture<CorfuInterClusterReplicationServerNode> discoveryCallback;

    private String pluginFilePath;

    private CorfuInterClusterReplicationServerNode interClusterReplicationService;

    private boolean shouldRun = true;

    private ServerContext serverContext;

    private String localCorfuEndpoint;

    private CorfuRuntime runtime;

    private LogReplicationContext replicationContext;

    private LogReplicationServer logReplicationServerHandler;

    /**
     * Constructor Discovery Service
     *
     * @param serverContext
     * @param discoveryCallback
     */
    public CorfuReplicationDiscoveryService(ServerContext serverContext, CorfuReplicationClusterManagerAdapter clusterManagerAdapter,
                                            CompletableFuture<CorfuInterClusterReplicationServerNode> discoveryCallback) {
        this.clusterManagerAdapter = clusterManagerAdapter;
        this.nodeId = serverContext.getNodeId();
        this.serverContext = serverContext;
        this.localEndpoint = serverContext.getLocalEndpoint();
        this.localHost =  NodeLocator.parseString(serverContext.getLocalEndpoint()).getHost();
        this.pluginFilePath = serverContext.getPluginConfigFilePath();
        this.discoveryCallback = discoveryCallback;
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
            log.info("Connecting to Cluster Manager adapter...");

            this.clusterManagerAdapter.connect(this);

            log.info("Fetch topology from Cluster Manager...");

            topologyDescriptor = new TopologyDescriptor(clusterManagerAdapter.fetchTopology());

            // Health check - confirm this node belongs to a cluster in the topology
            if (clusterPresentInTopology(topologyDescriptor)) {

                log.info("Node[{}] belongs to cluster, descriptor={}", localEndpoint,
                        localClusterDescriptor);

                buildLogReplicationContext();

                // Unblock server initialization retrieving context: topology + configuration
                discoveryCallback.complete(interClusterReplicationService);

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

    /**
     * Construct common log replication context.
     */
    private void buildLogReplicationContext() {
        // Through LogReplicationConfigAdapter retrieve system-specific configurations (including streams to replicate)
        LogReplicationConfig logReplicationConfig = getLogReplicationConfiguration(getCorfuRuntime());

        this.logReplicationMetadataManager = new LogReplicationMetadataManager(getCorfuRuntime(),
                topologyDescriptor.getTopologyConfigId(), localClusterDescriptor.getClusterId());

        logReplicationServerHandler = new LogReplicationServer(serverContext, logReplicationConfig,
                logReplicationMetadataManager, localCorfuEndpoint);
        logReplicationServerHandler.setActive(localClusterDescriptor.getRole().equals(ClusterRole.ACTIVE));

        this.interClusterReplicationService = new CorfuInterClusterReplicationServerNode(serverContext,
                logReplicationServerHandler, logReplicationConfig);

        this.replicationContext = new LogReplicationContext(logReplicationConfig, topologyDescriptor,
                localCorfuEndpoint, interClusterReplicationService.getRouter().getServerAdapter().getChannelContext());
    }

    /**
     * Retrieve a Corfu Runtime to connect to the local Corfu Datastore.
     */
    private CorfuRuntime getCorfuRuntime() {
        if (runtime == null) {
            localCorfuEndpoint = getCorfuEndpoint(localHost, localClusterDescriptor.getCorfuPort());
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
                new LogReplicationStreamNameTableManager(runtime, pluginFilePath);

        Set<String> streamsToReplicate = replicationStreamNameTableManager.getStreamsToReplicate();

        // TODO pankti: Check if version does not match.  If if does not, create an event for site discovery to
        //  do a snapshot sync.
        // TODO(Gabriela): pending review upgrade path (changes)
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
        if (!isLeader) {
            log.warn("Current node {} is not the lead node, log replication cannot be started.", localEndpoint);
            return;
        }

        switch (localClusterDescriptor.getRole()) {
            case ACTIVE:
                log.info("Start as Source (sender/replicator) on node {}.", localNodeDescriptor);
                // TODO(Gabriela): only one instance of CorfuReplicationManager
                replicationManager = new CorfuReplicationManager(topologyDescriptor, replicationContext,
                        localNodeDescriptor, logReplicationMetadataManager, pluginFilePath, getCorfuRuntime());
                replicationManager.start();
                break;
            case STANDBY:
                // Standby Site : the LogReplicationServer (server handler) will initiate the LogReplicationSinkManager
                log.info("Start as Sink (receiver) on node {} ", localNodeDescriptor);
                break;
            default:
                log.error("Log Replication not started on this cluster. Leader node {} belongs to cluster with {} role.",
                            localEndpoint, localClusterDescriptor.getRole());
                break;

        }
    }

    /**
     * Stop ongoing Log Replication
     */
    private void stopLogReplication() {
        if (isLeader && localClusterDescriptor.getRole() == ClusterRole.ACTIVE) {
            replicationManager.stop();
        }
    }

    /**
     * Process lock acquisition event
     */
    public void processLockAcquire() {
        log.info("Process lock acquire event");

        interClusterReplicationService.getLogReplicationServer().setLeadership(true);

        // TODO(Gabriela): confirm that start does not affect ongoing replication if it is called again..
        if (!isLeader) {
            // leader transition from false to true, start log replication.
            isLeader = true;
            startLogReplication();
        }
    }

    /**
     * Process lock release event
     *
     * Set leadership metadata and stop log replication in the event of leadership loss
     */
    public void processLockRelease() {
        log.info("Process lock release event");

        interClusterReplicationService.getLogReplicationServer().setLeadership(false);

        if (isLeader) {
            stopLogReplication();
            isLeader = false;
        }
    }

    public void processSiteFlip(TopologyDescriptor newConfig) {
        // stop ongoing replication, stopLogReplication() checks leadership and active
        stopLogReplication();

        //TODO pankti: read the configuration again and refresh the LogReplicationConfig object

        // update local topology descriptor
        topologyDescriptor = newConfig;

        // update local cluster descriptor
        localClusterDescriptor = topologyDescriptor.getClusterDescriptor(localEndpoint);

        // update local node descriptor
        localNodeDescriptor = localClusterDescriptor.getNode(localEndpoint);

        // update config id in metadata manager
        interClusterReplicationService.getLogReplicationServer().getSinkManager()
                .updateTopologyConfigId(topologyDescriptor.getTopologyConfigId());
        log.debug("Persist new topologyConfigId {}, status={}", topologyDescriptor.getTopologyConfigId(),
                localClusterDescriptor.getRole());

        logReplicationServerHandler.setActive(localClusterDescriptor.getRole().equals(ClusterRole.ACTIVE));

        // we do not need to update replication manager's config, since it will be initialized again

        startLogReplication();
    }

    public void processSiteChangeNotification(DiscoveryServiceEvent event) {
        // Stale notification, skip
        if (event.getTopologyConfig().getTopologyConfigID() < topologyDescriptor.getTopologyConfigId()) {
            log.debug("Stale Topology Change Notification, current={}, received={}", topologyDescriptor.getTopologyConfigId(), event.getTopologyConfig());
            return;
        }

        TopologyDescriptor newConfig = new TopologyDescriptor(clusterManagerAdapter.fetchTopology());
        if (newConfig.getTopologyConfigId() == topologyDescriptor.getTopologyConfigId()) {
            if (localClusterDescriptor.getRole() == ClusterRole.STANDBY) {
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

        if (!isLeader || localClusterDescriptor.getRole() != ClusterRole.ACTIVE) {
            return;
        }

        replicationManager.restart(event.getRemoteSiteInfo());
    }

    /***
     * After an upgrade, the active site should perform a snapshot sync
     */
    private void processUpgrade(DiscoveryServiceEvent event) {
        if (isLeader && localClusterDescriptor.getRole() == ClusterRole.ACTIVE) {
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
        if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE && replicationManager != null) {
            replicationManager.prepareSiteRoleChange();
        }
    }

    /**
     * Query the current all replication stream log tail and calculate the number of messages to be sent.
     * If the max tail has changed, give 0%. Otherwise,
     */
    @Override
    public int queryReplicationStatus() {
        if (localClusterDescriptor.getRole() == ClusterRole.ACTIVE) {
            // If not leader, replication manager might be null
            if (replicationManager != null) {
                return replicationManager.queryReplicationStatus();
            }
            return 0;
        } else {
            return -1;
        }
    }

    public void shutdown() {
        if (replicationManager != null) {
            replicationManager.stop();
        }

        if(clusterManagerAdapter != null) {
            clusterManagerAdapter.shutdown();
        }
    }
}
