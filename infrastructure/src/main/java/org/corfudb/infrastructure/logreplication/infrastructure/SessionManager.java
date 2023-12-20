package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.protobuf.TextFormat;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.infrastructure.msghandlers.LogReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.LogReplication.ReplicationSubscriber;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.METADATA_TABLE_NAME;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.addLegacyTypesToSerializer;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;

/**
 * Manage log replication sessions for multiple replication models.
 *
 * A replication session is determined by the cluster endpoints (source & sink)--given by the
 * Cluster Manager (topology)--the replication model and the client (given by clients upon
 * explicit registration or implicitly through protoBuf schema options).
 *
 * The access to the methods in this class is single threaded, but multiple threads will read from the data structures
 * containing the session: sessions, incomingSessions and outgoingSessions
 */
@Slf4j
public class SessionManager {
    private final CorfuStore corfuStore;

    private final String localCorfuEndpoint;

    private final CorfuReplicationManager replicationManager;

    private final LogReplicationConfigManager configManager;

    private final LogReplicationClientRegisterListener clientConfigListener;

    private final Set<LogReplicationSession> incomingSessions = ConcurrentHashMap.newKeySet();

    private final Set<LogReplicationSession> outgoingSessions = ConcurrentHashMap.newKeySet();

    private final Set<ReplicationSubscriber> processedSubscribers = ConcurrentHashMap.newKeySet();

    @Getter
    private TopologyDescriptor topology;

    @Getter
    private final LogReplicationMetadataManager metadataManager;

    @Getter
    private final LogReplicationContext replicationContext;

    @Getter
    private final LogReplicationClientServerRouter router;

    private final LogReplicationServer incomingMsgHandler;

    /**
     * Constructor
     *
     * @param topology            the current topology
     * @param corfuRuntime        runtime for database access
     * @param serverContext       the server context
     */
    public SessionManager(@Nonnull TopologyDescriptor topology, CorfuRuntime corfuRuntime,
                          ServerContext serverContext,
                          String localCorfuEndpoint, LogReplicationPluginConfig pluginConfig) {
        this.topology = topology;
        this.corfuStore = new CorfuStore(corfuRuntime);

        this.localCorfuEndpoint = localCorfuEndpoint;

        this.configManager = new LogReplicationConfigManager(corfuRuntime, serverContext,
                topology.getLocalClusterDescriptor().getClusterId());
        this.clientConfigListener = new LogReplicationClientRegisterListener(this,
                configManager, corfuStore);
        this.replicationContext = new LogReplicationContext(configManager, topology.getTopologyConfigId(),
                localCorfuEndpoint, pluginConfig, corfuRuntime);
        this.metadataManager = new LogReplicationMetadataManager(corfuRuntime, replicationContext);

        this.replicationManager = new CorfuReplicationManager(metadataManager,
                replicationContext);

        this.incomingMsgHandler = new LogReplicationServer(serverContext, metadataManager,
                topology.getLocalNodeDescriptor().getNodeId(), topology.getLocalNodeDescriptor().getClusterId(),
                replicationContext, this);

        this.router = new LogReplicationClientServerRouter(replicationManager,
                topology.getLocalNodeDescriptor().getClusterId(), topology.getLocalNodeDescriptor().getNodeId(),
                incomingMsgHandler, serverContext, pluginConfig);
    }

    /**
     * Test constructor.
     *
     * @param topology            the current topology
     * @param corfuRuntime        runtime for database access
     */
    @VisibleForTesting
    public SessionManager(@Nonnull TopologyDescriptor topology, CorfuRuntime corfuRuntime,
                          CorfuReplicationManager replicationManager, LogReplicationClientServerRouter router,
                          LogReplicationServer logReplicationServer, LogReplicationPluginConfig pluginConfig) {
        this.topology = topology;
        this.corfuStore = new CorfuStore(corfuRuntime);

        NodeLocator lrNodeLocator = NodeLocator.builder().host("localhost")
            .port(topology.getLocalClusterDescriptor().getCorfuPort())
            .build();
        this.localCorfuEndpoint = lrNodeLocator.toEndpointUrl();
        this.configManager = new LogReplicationConfigManager(corfuRuntime, topology.getLocalClusterDescriptor().getClusterId());
        this.clientConfigListener = new LogReplicationClientRegisterListener(this, configManager, corfuStore);
        this.replicationContext = new LogReplicationContext(configManager, topology.getTopologyConfigId(),
                localCorfuEndpoint, pluginConfig, corfuRuntime);
        this.metadataManager = new LogReplicationMetadataManager(corfuRuntime, replicationContext);
        this.replicationManager = replicationManager;
        this.router = router;
        this.incomingMsgHandler = logReplicationServer;
    }

    /**
     * Start client config listener from discovery service upon leadership acquired.
     */
    public void startClientConfigListener() {
        if (!this.clientConfigListener.listenerStarted()) {
            this.clientConfigListener.start();
        } else {
            log.warn("Client config listener already started on node {}", topology.getLocalNodeDescriptor().getNodeId());
        }
    }

    /**
     * Stop client config listener from discovery service upon leadership lost. Client config listener was running
     * on this node if it was the leader node.
     */
    public void stopClientConfigListener() {
        if (this.clientConfigListener.listenerStarted()) {
            log.debug("Stopping client config listener on node {}", topology.getLocalNodeDescriptor().getNodeId());
            this.clientConfigListener.stop();
        }
    }

    /**
     * Refresh sessions based on new topology :
     * (i) For sessions that are still valid, update the topology
     * (ii) For newly added clusters, create sessions, update internal tables, and create appropriate LR components
     * (iii) For a cluster that was removed, identify the (now) stale sessions, stop replication and clear the cached
     * information pertaining to the stale sessions
     *
     * The metadata updates will be done only by the leader node, so other nodes do not overwrite the data
     *
     * @param newTopology   the new discovered topology
     */
    public synchronized void refresh(@Nonnull TopologyDescriptor newTopology) {
        Set<String> newRemoteSources = newTopology.getRemoteSourceClusters().keySet();
        Set<String> currentRemoteSources = topology.getRemoteSourceClusters().keySet();
        Set<String> remoteSourcesToRemove = Sets.difference(currentRemoteSources, newRemoteSources);
        Set<String> remoteSourceClustersUnchanged = Sets.intersection(newRemoteSources, currentRemoteSources);

        Set<String> newRemoteSinks = newTopology.getRemoteSinkClusters().keySet();
        Set<String> currentRemoteSinks = topology.getRemoteSinkClusters().keySet();
        Set<String> remoteSinksToRemove = Sets.difference(currentRemoteSinks, newRemoteSinks);

        Set<LogReplicationSession> sessionsToRemove = new HashSet<>();
        Set<LogReplicationSession> sessionsUnchanged = new HashSet<>();

        Set<LogReplicationSession> allSessions = mergeIncomingOutgoingSessions();

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
                    allSessions.forEach(session -> {
                        if (remoteSinksToRemove.contains(session.getSinkClusterId()) ||
                                remoteSourcesToRemove.contains(session.getSourceClusterId())) {
                            sessionsToRemove.add(session);
                        } else {
                            sessionsUnchanged.add(session);
                            //update topologyId for sink sessions
                            if (remoteSourceClustersUnchanged.contains(session.getSourceClusterId())) {
                                metadataManager.updateReplicationMetadataField(txn, session,
                                        ReplicationMetadata.TOPOLOGYCONFIGID_FIELD_NUMBER, newTopology.getTopologyConfigId());
                            }
                        }
                    });
                    txn.commit();
                } catch (TransactionAbortedException e) {
                    throw new RetryNeededException();
                }
                stopReplication(sessionsToRemove);
                updateTopology(newTopology);
                updateReplicationParameters(Sets.intersection(sessionsUnchanged, outgoingSessions));
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception while refreshing sessions", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }

        // When a node acquires the lock, check if all the sessions present in the system tables are still valid.
        // Since the metadata table is written to only by the leader node, we can run into the scenario where
        // leadership was acquired after processing a topology change.
        // On acquiring leadership, stale sessions will not get removed from the metadata table as the in-memory copy
        // of sessions have already been updated.
        // Therefore, iterate over the sessions in the system tables and remove the stale sessions if any.
        if (replicationContext.getIsLeader().get()) {
            removeStaleSessionOnLeadershipAcquire();
        }

        // check if any new SINKs have been added to the topology.
        Set<String> newRemoteSinksAdded = Sets.difference(newRemoteSinks, currentRemoteSinks);
        if (!newRemoteSinksAdded.isEmpty()) {
            configManager.getRegisteredSubscribers().forEach(this::createSessions);
        } else {
            // Since sessions are created on all the nodes in the SOURCE cluster, its possible that on leadership change,
            // the new leader doesn't have any new sessions to create. In that case, only connect
            connectToRemoteClusters();
        }
    }

    private Set<LogReplicationSession> mergeIncomingOutgoingSessions() {
        Set<LogReplicationSession> allSessions = new HashSet<>();
        allSessions.addAll(outgoingSessions);
        allSessions.addAll(incomingSessions);
        return allSessions;
    }

    /**
     * Update in-memory topology and topologyId
     * @param newTopology new topology
     */
    private void updateTopology(TopologyDescriptor newTopology) {
        topology = newTopology;
        incomingMsgHandler.updateTopologyConfigId(topology.getTopologyConfigId());
        replicationContext.setTopologyConfigId(topology.getTopologyConfigId());
    }

    private void updateReplicationParameters(Set<LogReplicationSession> sessionsUnchanged) {
        if (replicationManager == null) {
            return;
        }
        for (LogReplicationSession session : sessionsUnchanged) {
            ClusterDescriptor cluster = topology.getRemoteSinkClusters().get(session.getSinkClusterId());
            replicationManager.refreshRuntime(session, cluster, topology.getTopologyConfigId());
        }
    }

    /**
     * Replication sessions are created by combining topology and registered replication subscribers.
     *
     * (1) In the case of replication start / topology change, check all the subscribers with underlying topology and
     *     create all the missed sessions.
     *
     * (2) In the case of client registration (on Source), sessions are created on-demand from client config listener.
     */
    public void createSessions(ReplicationSubscriber subscriber) {
        if (!replicationContext.getIsLeader().get()) {
            log.debug("Current Node is not the leader. Skipping session creation");
            return;
        }
        Set<LogReplicationSession> newSessions = new HashSet<>();
        try {
            String localClusterId = topology.getLocalClusterDescriptor().getClusterId();

            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
                    // Create out-going sessions by combing the subscriber and topology
                    for(ClusterDescriptor remoteSinkCluster : topology.getRemoteSinkClusters().values()) {
                        Set<ReplicationModel> supportedModels =
                                topology.getRemoteSinkClusterToReplicationModels().get(remoteSinkCluster);
                        if (supportedModels.contains(subscriber.getModel())) {
                            LogReplicationSession session =
                                    constructSession(localClusterId, remoteSinkCluster.clusterId, subscriber);
                            if (!outgoingSessions.contains(session)) {
                                newSessions.add(session);
                                metadataManager.addSession(txn, session, topology.getTopologyConfigId(), false);
                                processedSubscribers.add(subscriber);
                            } else {
                                log.warn("Trying to create an existed session: {}", TextFormat.shortDebugString(session));
                            }
                        }
                    }
                    txn.commit();

                    return null;
                } catch (TransactionAbortedException e) {
                    log.error("Failed to create sessions. Retrying.", e);
                    newSessions.clear();
                    throw new RetryNeededException();
                }
            }).run();

        } catch (InterruptedException e) {
            log.error("Unrecoverable Corfu Error when creating the sessions", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
        outgoingSessions.addAll(newSessions);
        log.info("Total of {} outgoing sessions created with subscriber {}, sessions={}", newSessions.size(),
                subscriber, newSessions);

        processNewSessions(newSessions);
    }

    private void processNewSessions(Set<LogReplicationSession> newSessions) {
        configManager.generateConfig(newSessions, true);
        newSessions.stream().filter(outgoingSessions::contains)
                .forEach(session -> router.processNewSessions(session, topology.getRemoteSinkClusters().get(session.getSinkClusterId())));
        createSourceFSMs(newSessions);
        // TODO(V2): The below method logs a mapping of a session's hashcode to the session itself.  This
        //  is because LR names worker threads corresponding to a session using its hashcode.
        //  To identify the remote cluster and replication information while debugging, we need a view of
        //  the whole session object.  However, this method of logging the mapping has limitations
        //  because the mapping is only printed when the session is created for the first time.  If the
        //  logs have rolled over, the information is lost.  We need a permanent store/log of this mapping.
        logNewlyAddedSessionInfo(newSessions);
        connectToRemoteClusters();
    }

    public void addNotifiedSessionFromSource(LogReplicationSession sessionFromSource) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
                    metadataManager.addSession(txn, sessionFromSource, topology.getTopologyConfigId(), true);
                    txn.commit();
                } catch (TransactionAbortedException e) {
                    log.error("Failed to create sessions. Retrying.", e);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable Corfu Error when creating the sessions", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
        incomingSessions.add(sessionFromSource);
        log.info("Incoming session from source registered with session {}", sessionFromSource);
        processNewSessions(Collections.singleton(sessionFromSource));
    }

    private void logNewlyAddedSessionInfo(Set<LogReplicationSession> newSessions) {
        log.info("========= HashCode -> Session mapping for newly added session: =========");
        for (LogReplicationSession session : newSessions) {
            log.info("HashCode: {}, Session: {}", session.hashCode(), session);
        }
    }

    /**
     * Construct a replication session.
     */
    private LogReplicationSession constructSession(String sourceClusterId, String sinkClusterId,
                                                   ReplicationSubscriber subscriber) {
        return LogReplicationSession.newBuilder()
                .setSourceClusterId(sourceClusterId)
                .setSinkClusterId(sinkClusterId)
                .setSubscriber(subscriber)
                .build();
    }

    private void removeStaleSessionOnLeadershipAcquire() {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = metadataManager.getTxnContext()) {
                    Set<LogReplicationSession> sessionsInTable = txn.keySet(METADATA_TABLE_NAME);
                    sessionsInTable.addAll(txn.keySet(REPLICATION_STATUS_TABLE_NAME));
                    sessionsInTable.stream()
                            .filter(sessionInTable -> !outgoingSessions.contains(sessionInTable) && !incomingSessions.contains(sessionInTable))
                            .forEach(staleSession -> metadataManager.removeSession(txn, staleSession));
                    txn.commit();
                    return null;
                } catch (TransactionAbortedException e) {
                    log.error("Failed to create sessions.  Retrying.", e);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable Corfu Error when removing stale sessions from LR internal tables", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Retrieve all sessions for which this cluster is the sink
     *
     * @return set of sessions for which this cluster is the sink
     */
    public Set<LogReplicationSession> getIncomingSessions() {
        return incomingSessions;
    }

    /**
     * Retrieve all sessions for which this cluster is the source
     *
     * @return set of sessions for which this cluster is the sink
     */
    public Set<LogReplicationSession> getOutgoingSessions() {
        return outgoingSessions;
    }

    /**
     * Retrieve replication status for all sessions
     *
     * @return  map of replication status per session
     */
    public Map<LogReplicationSession, ReplicationStatus> getReplicationStatus() {
        return metadataManager.getReplicationStatus();
    }

    /**
     * Stop all sessions
     */
    public void stopReplication() {
        log.info("Stopping log replication.");
        // Stop config listener if required
        stopClientConfigListener();
        stopReplication(mergeIncomingOutgoingSessions());
    }

    /**
     * Stop replication for a set of sessions and clear in-memory info
     * @param sessions
     */
    private void stopReplication(Set<LogReplicationSession> sessions) {
        if (sessions.isEmpty()) {
            return;
        }
        if (replicationManager != null) {
            // stop replication fsm and source router for outgoing sessions
            replicationManager.stop(sessions.stream().filter(outgoingSessions::contains).collect(Collectors.toSet()));
            router.stop(sessions);
        }

        sessions.forEach(session -> {
            if (incomingSessions.contains(session)) {
                incomingMsgHandler.stopSinkManagerForSession(session);
            }
            this.outgoingSessions.remove(session);
            this.incomingSessions.remove(session);
            this.processedSubscribers.remove(session.getSubscriber());
        });

    }

    /**
     * The SOURCE connects to the SINK for any session
     */
    public synchronized void connectToRemoteClusters() {
        // Connect to all the discovered sessions on becoming a leader. Connection for an already connected session is
        // skipped in the router.
        if (!replicationContext.getIsLeader().get()) {
            return;
        }
        outgoingSessions.forEach(session -> router.connect(topology.getAllClustersInTopology().get(session.getSinkClusterId()), session));
    }

    /**
     * Create runtimeFSM for sessions for which the local cluster is a SOURCE
     */
    private void createSourceFSMs(Set<LogReplicationSession> newSessions) {
        newSessions.stream()
                .filter(outgoingSessions::contains)
                .forEach(session -> replicationManager.createAndStartRuntime(session, router));
    }

    /**
     * Shutdown session manager
     */
    public void shutdown() {
        replicationManager.stop();
        router.stop(outgoingSessions);
        router.shutDownMsgHandlerServer();
    }

    /**
     * Force snapshot sync for the replication session associated with the DiscoveryServiceEvent. Note that this
     * method is executed from external path - The forced snapshot sync event is triggered by the client of log
     * replicator.
     *
     * @param event DiscoveryServiceEvent that specifies the session for forced snapshot sync.
     */
    public void enforceSnapshotSync(DiscoveryServiceEvent event) {
        replicationManager.enforceSnapshotSync(event);
    }

    /**
     * Notify LogReplicationServer about the leadership change so that sink managers can be reset/stopped accordingly.
     */
    public void notifyLeadershipChange() {
        incomingMsgHandler.leadershipChanged();
    }
}
