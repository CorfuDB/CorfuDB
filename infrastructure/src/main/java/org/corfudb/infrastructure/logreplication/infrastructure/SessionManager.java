package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.protobuf.TextFormat;
import com.google.protobuf.Timestamp;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent.ReplicationEventType;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEventInfoKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationMetadata;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationUpgradeManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
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
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.ObjectsView.DEFAULT_LOGICAL_GROUP_CLIENT;

/**
 * Manage log replication sessions for multiple replication models.
 *
 * A replication session is determined by the cluster endpoints (source & sink)--given by the
 * Cluster Manager (topology)--the replication model and the client (given by clients upon
 * explicit registration or implicitly through protoBuf schema options).
 */
@Slf4j
public class SessionManager {

    // Represents default client for LR V1, i.e., use case where tables are tagged with
    // 'is_federated' flag, yet no client is specified in proto
    private static final String DEFAULT_CLIENT = "00000000-0000-0000-0000-000000000000";

    private final CorfuStore corfuStore;

    private final CorfuRuntime runtime;

    private final String localCorfuEndpoint;

    private CorfuReplicationManager replicationManager;

    private final LogReplicationConfigManager configManager;

    @Getter
    private final Set<LogReplicationSession> sessions = new CopyOnWriteArraySet<>();

    private final Set<LogReplicationSession> incomingSessions = new CopyOnWriteArraySet<>();

    private final Set<LogReplicationSession> outgoingSessions = new CopyOnWriteArraySet<>();

    @Getter
    private TopologyDescriptor topology;

    @Getter
    private final LogReplicationMetadataManager metadataManager;

    private final LogReplicationUpgradeManager upgradeManager;

    @Getter
    private final LogReplicationContext replicationContext;

    /**
     * Constructor
     *
     * @param topology            the current topology
     * @param corfuRuntime        runtime for database access
     * @param serverContext       the server context
     * @param upgradeManager      upgrade management module
     */
    public SessionManager(@Nonnull TopologyDescriptor topology, CorfuRuntime corfuRuntime,
                          ServerContext serverContext, LogReplicationUpgradeManager upgradeManager) {
        this.topology = topology;
        this.runtime = corfuRuntime;
        this.corfuStore = new CorfuStore(corfuRuntime);

        // serverContext.getLocalEndpoint currently provides a string with address = localhost and port = LR server
        // port.  But the endpoint needed here is with the Corfu port, i.e., 9000.  So create a NodeLocator
        // using the endpoint available from serverContext first and then create a new NodeLocator which replaces the
        // port with the Corfu port.
        // TODO: Once IPv6 changes are available, the 2nd NodeLocator need not be created.  A utility method which
        //  generates the endpoint given an IP address and port - getVersionFormattedHostAddressWithPort() - should be
        //  used
        NodeLocator nodeLocator = NodeLocator.parseString(serverContext.getLocalEndpoint());
        NodeLocator lrNodeLocator = NodeLocator.builder().host(nodeLocator.getHost())
                .port(topology.getLocalClusterDescriptor().getCorfuPort())
                .build();

        this.localCorfuEndpoint = lrNodeLocator.toEndpointUrl();

        this.metadataManager = new LogReplicationMetadataManager(corfuRuntime, topology.getTopologyConfigId());
        this.configManager = new LogReplicationConfigManager(runtime, serverContext,
                topology.getLocalClusterDescriptor().getClusterId());
        this.upgradeManager = upgradeManager;
        replicationContext = new LogReplicationContext(configManager, topology.getTopologyConfigId(), localCorfuEndpoint);

        createSessions();
    }

    /**
     * Test constructor.
     *
     * @param topology            the current topology
     * @param corfuRuntime        runtime for database access
     */
    @VisibleForTesting
    public SessionManager(@Nonnull TopologyDescriptor topology, CorfuRuntime corfuRuntime) {
        this.topology = topology;
        this.runtime = corfuRuntime;
        this.corfuStore = new CorfuStore(corfuRuntime);

        NodeLocator lrNodeLocator = NodeLocator.builder().host("localhost")
            .port(topology.getLocalClusterDescriptor().getCorfuPort())
            .build();
        this.localCorfuEndpoint = lrNodeLocator.toEndpointUrl();
        this.metadataManager = new LogReplicationMetadataManager(corfuRuntime, topology.getTopologyConfigId());
        this.configManager = new LogReplicationConfigManager(runtime, topology.getLocalClusterDescriptor().getClusterId());
        this.upgradeManager = null;
        replicationContext = new LogReplicationContext(configManager, topology.getTopologyConfigId(), localCorfuEndpoint);

        createSessions();
    }

    /**
     * Refresh sessions based on new topoloogy
     *
     * @param newTopology   the new discovered topology
     */
    // TODO [V2]: In the connectionModel PR, ensure that refresh is called by the leader node only. Otherwise the
    //  metadata/status tables will be overwritten by all the nodes in the cluster
    public synchronized void refresh(@Nonnull TopologyDescriptor newTopology) {

        // TODO V2: Make this method a no-op if the new topology has not changed.  Need to override equals and
        //  hashcode in all TopologyDescriptor and all its members.  It is being taken care of in a subsequent PR
        //  which contains changes to the Connection Model.

        Set<String> newRemoteSources = newTopology.getRemoteSourceClusters().keySet();
        Set<String> currentRemoteSources = topology.getRemoteSourceClusters().keySet();
        Set<String> remoteSourcesToRemove = Sets.difference(currentRemoteSources, newRemoteSources);
        Set<String> remoteSourceClustersUnchanged = Sets.intersection(newRemoteSources, currentRemoteSources);

        Set<String> newRemoteSinks = newTopology.getRemoteSinkClusters().keySet();
        Set<String> currentRemoteSinks = topology.getRemoteSinkClusters().keySet();
        Set<String> remoteSinksToRemove = Sets.difference(currentRemoteSinks, newRemoteSinks);

        Set<LogReplicationSession> sessionsToRemove = new HashSet<>();
        Set<LogReplicationSession> sessionsUnchanged = new HashSet<>();

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
                    sessions.forEach(session -> {
                        if (remoteSinksToRemove.contains(session.getSinkClusterId()) ||
                                remoteSourcesToRemove.contains(session.getSourceClusterId())) {
                            sessionsToRemove.add(session);
                            metadataManager.removeSession(txn, session);
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

                // Update the in-memory topology config id after metadata tables have been updated
                topology = newTopology;
                replicationContext.setTopologyConfigId(topology.getTopologyConfigId());
                metadataManager.setTopologyConfigId(topology.getTopologyConfigId());

                stopReplication(sessionsToRemove);

                updateReplicationParameters(Sets.intersection(sessionsUnchanged, outgoingSessions));
                createSessions();
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception while refreshing sessions", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    private void updateReplicationParameters(Set<LogReplicationSession> sessionsUnchanged) {
        if (replicationManager == null) {
            return;
        }
        for (LogReplicationSession session : sessionsUnchanged) {
            ClusterDescriptor cluster = topology.getRemoteSinkClusters().get(session.getSourceClusterId());
            replicationManager.refreshRuntime(session, cluster, topology.getTopologyConfigId());
        }
    }

    /**
     * Replication sessions are created by combining topology and registered replication subscribers.
     *
     * (1) In the case of replication start / topology change, check all the subscribers with underlying topology and
     *     create all the missed sessions.
     *
     * (2) In the case of client registration (on Source), sessions are created on-demand:
     *     Source side: invoke session creation method from client config listener
     *     Sink side: assigned grpc stream for subscriber registration
     */
    private void createSessions() {
        for (ReplicationSubscriber subscriber : configManager.getRegisteredSubscribers()) {
            createOutgoingSessionsBySubscriber(subscriber);
            createIncomingSessionsBySubscriber(subscriber);
        }
    }

    public void createOutgoingSessionsBySubscriber(ReplicationSubscriber subscriber) {
        Set<LogReplicationSession> sessionsToAdd = new HashSet<>();
        try {
            String localClusterId = topology.getLocalClusterDescriptor().getClusterId();
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
                    // Create out-going sessions by combing the given registered replication subscriber and topology
                    for(ClusterDescriptor remoteSinkCluster : topology.getRemoteSinkClusters().values()) {
                        Set<ReplicationModel> supportedModels =
                                topology.getRemoteSinkClusterToReplicationModels().get(remoteSinkCluster);
                        if (supportedModels.contains(subscriber.getModel())) {
                            LogReplicationSession session =
                                    constructSession(localClusterId, remoteSinkCluster.clusterId, subscriber);
                            // TODO: (V2 / Chris) this is still not thread-safe, need to synchronize on sessions
                            if (!sessions.contains(session)) {
                                sessionsToAdd.add(session);
                                metadataManager.addSession(txn, session, topology.getTopologyConfigId(), false);
                            } else {
                                log.warn("Trying to create an existed session: {}", TextFormat.shortDebugString(session));
                            }
                        }
                    }
                    txn.commit();
                    sessions.addAll(sessionsToAdd);
                    outgoingSessions.addAll(sessionsToAdd);
                    logNewlyAddedSessionInfo(sessionsToAdd);
                    return null;
                } catch (TransactionAbortedException e) {
                    log.error("Failed to create sessions.  Retrying.", e);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable Corfu Error when creating the sessions", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }

        log.info("Total of {} outgoing sessions created with subscriber {}, sessions={}", sessionsToAdd.size(),
                subscriber, sessionsToAdd);

        configManager.generateConfig(sessionsToAdd);
    }

    public void createIncomingSessionsBySubscriber(ReplicationSubscriber subscriber) {
        Set<LogReplicationSession> sessionsToAdd = new HashSet<>();
        try {
            String localClusterId = topology.getLocalClusterDescriptor().getClusterId();
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
                    // Create out-going sessions by combing the given registered replication subscriber and topology
                    for(ClusterDescriptor remoteSourceCluster : topology.getRemoteSourceClusters().values()) {
                        Set<ReplicationModel> supportedModels =
                                topology.getRemoteSourceClusterToReplicationModels().get(remoteSourceCluster);
                        if (supportedModels.contains(subscriber.getModel())) {
                            LogReplicationSession session =
                                    constructSession(remoteSourceCluster.clusterId, localClusterId, subscriber);
                            // TODO: (V2 / Chris) this is still not thread-safe, need to synchronize on sessions
                            if (!sessions.contains(session)) {
                                sessionsToAdd.add(session);
                                metadataManager.addSession(txn, session, topology.getTopologyConfigId(), true);
                            }
                        }
                    }
                    txn.commit();
                    sessions.addAll(sessionsToAdd);
                    incomingSessions.addAll(sessionsToAdd);
                    logNewlyAddedSessionInfo(sessionsToAdd);
                    return null;
                } catch (TransactionAbortedException e) {
                    log.error("Failed to create sessions.  Retrying.", e);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable Corfu Error when creating the sessions", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }

        log.info("Total of {} incoming sessions created with subscriber {}, sessions={}", sessionsToAdd.size(),
                subscriber, sessionsToAdd);

        configManager.generateConfig(sessionsToAdd);
    }

    // TODO(V2): The below method logs a mapping of a session's hashcode to the session itself.  This
    //  is because LR names worker threads corresponding to a session using its hashcode.
    //  To identify the remote cluster and replication information while debugging, we need a view of
    //  the whole session object.  However, this method of logging the mapping has limitations
    //  because the mapping is only printed when the session is created for the first time.  If the
    //  logs have rolled over, the information is lost.  We need a permanent store/log of this mapping.
    private synchronized void logNewlyAddedSessionInfo(Set<LogReplicationSession> newlyAddedSessions) {
        log.info("========= HashCode -> Session mapping for newly added session: =========");
        for (LogReplicationSession session : newlyAddedSessions) {
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
     * Reset replication status for all sessions
     */
    public void resetReplicationStatus() {
        metadataManager.resetReplicationStatus();
    }

    /**
     * Retrieve replication status for all sessions
     *
     * @return  map of replication status per session
     */
    public Map<LogReplicationSession, ReplicationStatus> getReplicationStatus() {
        return metadataManager.getReplicationStatus();
    }

    public static ReplicationSubscriber getDefaultSubscriber() {
        return ReplicationSubscriber.newBuilder()
                .setClientName(DEFAULT_CLIENT)
                .setModel(ReplicationModel.FULL_TABLE)
                .build();
    }

    // TODO (V2): This builder should be removed after the rpc stream is added for Sink side session creation.
    public static ReplicationSubscriber getDefaultLogicalGroupSubscriber() {
        return ReplicationSubscriber.newBuilder()
                .setClientName(DEFAULT_LOGICAL_GROUP_CLIENT)
                .setModel(ReplicationModel.LOGICAL_GROUPS)
                .build();
    }

    public void stopReplication() {
        log.info("Stopping log replication.");
        replicationManager.stop();
    }

    private void stopReplication(Set<LogReplicationSession> sessions) {
        if (replicationManager != null) {
            replicationManager.stop(sessions.stream().filter(outgoingSessions::contains).collect(Collectors.toSet()));
        }

        sessions.forEach(session -> {
            this.sessions.remove(session);
            this.outgoingSessions.remove(session);
        });

    }

    public synchronized void startReplication() {

        log.info("Start replication for all sessions count={}", outgoingSessions.size());

        if(replicationManager == null) {
            this.replicationManager = new CorfuReplicationManager(topology.getLocalNodeDescriptor(),
                    metadataManager, configManager.getServerContext().getPluginConfigFilePath(), runtime, upgradeManager);
        }

        LogReplicationClientConfigListener clientConfigListener =
                new LogReplicationClientConfigListener(this, configManager, corfuStore);
        clientConfigListener.start();

        createSessions();
        outgoingSessions.forEach(session -> {
            ClusterDescriptor remoteClusterDescriptor = topology.getRemoteSinkClusters().get(session.getSinkClusterId());
            replicationManager.start(remoteClusterDescriptor, session, replicationContext);
        });
    }

    /**
     * Shutdown session manager
     */
    public void shutdown() {
        if (replicationManager != null) {
            replicationManager.stop();
        }
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
     * TODO (V2): reason enum to differentiate the paths that trigger a forced snapshot sync
     * Forced snapshot sync for a certain replication session. Note that this method is internal to LR for cases where
     * a replication session has significant config change such that a forced snapshot sync is needed.
     *
     * For example, for LOGICAL_GROUP model, if a new group is added to a Sink destination that has an on-going logical
     * group replication session, a forced snapshot sync should be triggered for the session to keep the Sink consistent
     * with Source side.
     *
     * @param session Replication session that a forced snapshot sync is needed because of config change.
     */
    public void enforceSnapshotSync(LogReplicationSession session) {
        UUID forceSyncId = UUID.randomUUID();

        log.info("Forced snapshot sync will be triggered because of group destination change, session={}, sync_id={}",
                session, forceSyncId);

        // Write a force sync event to the logReplicationEventTable
        ReplicationEventInfoKey key = ReplicationEventInfoKey.newBuilder()
                .setSession(session)
                .build();

        ReplicationEvent event = ReplicationEvent.newBuilder()
                .setEventId(forceSyncId.toString())
                .setType(ReplicationEventType.FORCE_SNAPSHOT_SYNC)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .build();

        metadataManager.addEvent(key, event);
    }
}
