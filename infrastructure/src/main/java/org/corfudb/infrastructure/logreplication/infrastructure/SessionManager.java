package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

    private String localCorfuEndpoint;

    private CorfuReplicationManager replicationManager;

    private LogReplicationConfigManager configManager;

    @Getter
    private Set<LogReplicationSession> sessions = new HashSet<>();

    private Set<LogReplicationSession> incomingSessions = new HashSet<>();

    private Set<LogReplicationSession> outgoingSessions = new HashSet<>();

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
        this.configManager = new LogReplicationConfigManager(runtime, serverContext);
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
        this.configManager = new LogReplicationConfigManager(runtime);
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

        Set<String> newSources = newTopology.getRemoteSourceClusters().keySet();
        Set<String> currentSources = topology.getRemoteSourceClusters().keySet();
        Set<String> sourcesToRemove = Sets.difference(currentSources, newSources);

        Set<String> newSinks = newTopology.getRemoteSinkClusters().keySet();
        Set<String> currentSinks = topology.getRemoteSinkClusters().keySet();
        Set<String> sinksToRemove = Sets.difference(currentSinks, newSinks);
        Set<String> sinkClustersUnchanged = Sets.intersection(newSinks, currentSinks);

        Set<LogReplicationSession> sessionsToRemove = new HashSet<>();
        Set<LogReplicationSession> sessionsUnchanged = new HashSet<>();

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
                    sessions.forEach(session -> {
                        if (sinksToRemove.contains(session.getSinkClusterId()) ||
                                sourcesToRemove.contains(session.getSourceClusterId())) {
                            sessionsToRemove.add(session);
                            metadataManager.removeSession(txn, session);
                        }

                        if(sinkClustersUnchanged.contains(session.getSinkClusterId())) {
                            sessionsUnchanged.add(session);
                            metadataManager.updateReplicationMetadataField(txn, session,
                                ReplicationMetadata.TOPOLOGYCONFIGID_FIELD_NUMBER, newTopology.getTopologyConfigId());
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

                updateReplicationParameters(sessionsUnchanged);
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
            ClusterDescriptor cluster = topology.getRemoteSinkClusters().get(session.getSinkClusterId());
            replicationManager.refreshRuntime(session, cluster, topology.getTopologyConfigId());
        }
    }

    /**
     * Create sessions (outgoing and incoming) along with metadata
     *
     */
    private void createSessions() {
        Set<LogReplicationSession> sessionsToAdd = new HashSet<>();
        Set<LogReplicationSession> incomingSessionsToAdd = new HashSet<>();
        Set<LogReplicationSession> outgoingSessionsToAdd = new HashSet<>();

        try {
            String localClusterId = topology.getLocalClusterDescriptor().getClusterId();

            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {

                    for(ClusterDescriptor remoteSinkCluster : topology.getRemoteSinkClusters().values()) {
                        // TODO(V2): for now only creating sessions for FULL TABLE replication model (assumed as default)
                        LogReplicationSession session = constructSession(localClusterId, remoteSinkCluster.clusterId);
                        if (!sessions.contains(session)) {
                            sessionsToAdd.add(session);
                            outgoingSessionsToAdd.add(session);
                            metadataManager.addSession(txn, session, topology.getTopologyConfigId(), false);
                        }
                    }

                    for(ClusterDescriptor remoteSourceCluster : topology.getRemoteSourceClusters().values()) {
                        // TODO(V2): for now only creating sessions for FULL TABLE replication model (assumed as default)
                        LogReplicationSession session = constructSession(remoteSourceCluster.getClusterId(), localClusterId);
                        if (!sessions.contains(session)) {
                            sessionsToAdd.add(session);
                            incomingSessionsToAdd.add(session);
                            metadataManager.addSession(txn, session, topology.getTopologyConfigId(), true);
                        }
                    }
                    txn.commit();

                    sessions.addAll(sessionsToAdd);
                    // TODO(V2): The below method logs a mapping of a session's hashcode to the session itself.  This
                    //  is because LR names worker threads corresponding to a session using its hashcode.
                    //  To identify the remote cluster and replication information while debugging, we need a view of
                    //  the whole session object.  However, this method of logging the mapping has limitations
                    //  because the mapping is only printed when the session is created for the first time.  If the
                    //  logs have rolled over, the information is lost.  We need a permanent store/log of this mapping.
                    logNewlyAddedSessionInfo(sessionsToAdd);
                    incomingSessions.addAll(incomingSessionsToAdd);
                    outgoingSessions.addAll(outgoingSessionsToAdd);
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

        log.info("Total sessions={}, outgoing={}, incoming={}, sessions={}", sessions.size(), outgoingSessions.size(),
                incomingSessions.size(), sessions);
    }


    private void logNewlyAddedSessionInfo(Set<LogReplicationSession> newlyAddedSessions) {
        log.info("========= HashCode -> Session mapping for newly added session: =========");
        for (LogReplicationSession session : newlyAddedSessions) {
            log.info("HashCode: {}, Session: {}", session.hashCode(), session);
        }
    }

    /**
     * Construct session.
     */
    private LogReplicationSession constructSession(String sourceClusterId, String sinkClusterId) {
        return LogReplicationSession.newBuilder()
                .setSourceClusterId(sourceClusterId)
                .setSinkClusterId(sinkClusterId)
                .setSubscriber(getDefaultSubscriber())
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

    public void stopReplication() {
        log.info("Stopping log replication.");
        replicationManager.stop();
    }

    private void stopReplication(Set<LogReplicationSession> sessions) {
        if (replicationManager != null) {
            replicationManager.stop(sessions);
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
     * Force snapshot sync on all outgoing sessions
     *
     * @param event
     */
    public void enforceSnapshotSync(DiscoveryServiceEvent event) {
        replicationManager.enforceSnapshotSync(event);
    }
}
