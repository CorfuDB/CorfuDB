package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatus;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.proto.service.CorfuMessage.ReplicationSubscriber;
import org.corfudb.runtime.proto.service.CorfuMessage.ReplicationModel;
import org.corfudb.runtime.proto.service.CorfuMessage.LogReplicationSession;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
    public static final String DEFAULT_CLIENT = "00000000-0000-0000-0000-000000000000";

    public static UUID DEFAULT_CLIENT_ID = UUID.fromString(DEFAULT_CLIENT);

    private final CorfuStore corfuStore;

    private final CorfuRuntime runtime;

    private String localCorfuEndpoint;

    private CorfuReplicationManager replicationManager;

    private LogReplicationConfigManager configManager;

    private Set<LogReplicationSession> sessions = new HashSet<>();

    private Set<LogReplicationSession> incomingSessions = new HashSet<>();

    private Set<LogReplicationSession> outgoingSessions = new HashSet<>();

    Map<LogReplicationSession, LogReplicationContext> sessionToContextMap = new HashMap<>();

    @Getter
    private TopologyDescriptor topology;

    @Getter
    private final LogReplicationMetadataManager metadataManager;

    /**
     * Constructor
     *
     * @param topology            the current topology
     * @param corfuRuntime        runtime for database access
     * @param serverContext       the server context
     */
    public SessionManager(@Nonnull TopologyDescriptor topology, CorfuRuntime corfuRuntime, ServerContext serverContext) {
        this.topology = topology;
        this.runtime = corfuRuntime;
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.localCorfuEndpoint = NodeLocator.parseString(serverContext.getLocalEndpoint()).getHost() + ":" +
                topology.getLocalClusterDescriptor().getCorfuPort();
        this.metadataManager =  new LogReplicationMetadataManager(corfuRuntime);
        this.configManager = new LogReplicationConfigManager(runtime, serverContext);

        createSessions(false);
    }

    /**
     * Refresh sessions based on new topoloogy
     *
     * @param newTopology   the new discovered topology
     */
    public synchronized void refresh(@Nonnull TopologyDescriptor newTopology, boolean isLeader) {

        // TODO pankti: Make this method a no-op if the new topology has not changed.  Need to override equals and
        //  hashcode in all TopologyDescriptor and all its members.

        Set<String> newSources = newTopology.getSourceClusters().keySet();
        Set<String> currentSources = topology.getSourceClusters().keySet();
        Set<String> sourcesToRemove = Sets.difference(currentSources, newSources);

        Set<String> newSinks = newTopology.getSinkClusters().keySet();
        Set<String> currentSinks = topology.getSinkClusters().keySet();
        Set<String> sinksToRemove = Sets.difference(currentSinks, newSinks);
        Set<String> sinkClustersUnchanged = Sets.intersection(newSinks, currentSinks);

        topology = newTopology;

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
                            sessionToContextMap.get(session).setTopologyConfigId(topology.getTopologyConfigId());
                            metadataManager.updateReplicationMetadataField(txn, session,
                                    ReplicationMetadata.TOPOLOGYCONFIGID_FIELD_NUMBER, topology.getTopologyConfigId());
                        }
                    });
                    txn.commit();
                } catch (TransactionAbortedException e) {
                    throw new RetryNeededException();
                }

                stopReplication(sessionsToRemove, true);
                updateReplicationParameters(sessionsUnchanged);
                createSessions(isLeader);
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception while removing session", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    private void updateReplicationParameters(Set<LogReplicationSession> sessionsUnchanged) {
        if (replicationManager == null) {
            return;
        }
        for (LogReplicationSession session : sessionsUnchanged) {
            ClusterDescriptor cluster = topology.getSinkClusters().get(session.getSinkClusterId());
            replicationManager.refreshRuntime(session, cluster, topology.getTopologyConfigId());
        }
    }

    /**
     * Create sessions (outgoing and incoming) along with metadata
     *
     * @param isLeader  true, if current node is the leader; false, otherwise
     *                  this flag indicates if replication should start for created sessions or not
     */
    private void createSessions(boolean isLeader) {
        LogReplicationSession session;

        try(TxnContext txn = corfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            // TODO(V2): A replication context should be specific per session, as the set of streams to replicate is
            // specific to a session (for now only supporting one default model) so using the same context
            LogReplicationContext context = new LogReplicationContext(configManager,
                    topology.getTopologyConfigId(), localCorfuEndpoint);

            for(ClusterDescriptor sourceCluster : topology.getSourceClusters().values()) {
                for(ClusterDescriptor sinkCluster : topology.getSinkClusters().values()) {

                    // TODO(V2): for now only creating sessions for FULL TABLE replication model (assumed as default)
                    session = LogReplicationSession.newBuilder()
                            .setSourceClusterId(sourceCluster.clusterId)
                            .setSinkClusterId(sinkCluster.clusterId)
                            .setSubscriber(getDefaultSubscriber())
                            .build();

                    if(!sessions.contains(session)) {
                        if (session.getSourceClusterId() == topology.getLocalClusterDescriptor().getClusterId()) {
                            sessions.add(session);
                            outgoingSessions.add(session);
                            metadataManager.addSession(txn, session, topology.getTopologyConfigId(), false);

                            /*if(isLeader) {
                                replicationManager.start(topology.getSinkClusters().get(session.getSinkClusterId()),
                                        session, context);
                            }*/
                        } else if(session.getSinkClusterId() == topology.getLocalClusterDescriptor().getClusterId()) {
                            sessions.add(session);
                            incomingSessions.add(session);
                            metadataManager.addSession(txn, session, topology.getTopologyConfigId(), true);
                        } else {
                            log.info("Session {} does not contain current node {} - skipping", session,
                                    topology.getLocalClusterDescriptor().getClusterId());
                        }

                        sessionToContextMap.put(session, context);
                    }
                }
            }
            txn.commit();
        }

        log.info("Total sessions={}, outgoing={}, incoming={}, sessions={}", sessions.size(), outgoingSessions.size(),
                incomingSessions.size(), sessions);
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
     * Retrieve all sessions from the given source cluster
     *
     * @param sourceClusterId   the identifier of the source cluster
     * @return set of sessions for which this cluster is the sink
     */
    public Set<LogReplicationSession> getIncomingSessions(String sourceClusterId) {
        return incomingSessions.stream()
                .filter(s -> s.getSinkClusterId().equals(sourceClusterId)).collect(Collectors.toSet());
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
     * Retrieve all sessions to the given sink cluster
     *
     * @param sinkClusterId   the identifier of the sink cluster
     * @return set of sessions for which this cluster is the sink
     */
    public Set<LogReplicationSession> getOutgoingSessions(String sinkClusterId) {
        return outgoingSessions.stream()
                .filter(s -> s.getSinkClusterId().equals(sinkClusterId)).collect(Collectors.toSet());
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
                .setClientId(UuidMsg.newBuilder()
                        .setMsb(DEFAULT_CLIENT_ID.getMostSignificantBits())
                        .setLsb(DEFAULT_CLIENT_ID.getMostSignificantBits())
                        .build())
                .setClientName(DEFAULT_CLIENT)
                .setModel(ReplicationModel.FULL_TABLE)
                .build();
    }

    public void stopReplication() {
        log.info("Stopping log replication.");
        replicationManager.stop();
    }

    public void stopReplication(Set<LogReplicationSession> sessions, boolean remove) {

        if (replicationManager != null) {
            replicationManager.stop(sessions);
        }

        if(remove) {
            sessions.forEach(session -> {
                this.sessions.remove(session);
                this.outgoingSessions.remove(session);
                this.sessionToContextMap.remove(session);
            });
        }
    }

    public LogReplicationContext getContext(LogReplicationSession session) {
        if(!sessionToContextMap.containsKey(session)) {
            log.warn("Context not found fpr session={}, generate new context");
            return new LogReplicationContext(configManager, topology.getTopologyConfigId(), localCorfuEndpoint);
        }
        return sessionToContextMap.get(session);
    }

    public synchronized void startReplication() {

        log.info("Start replication for all sessions count={}", outgoingSessions.size());

        if(replicationManager == null) {
            this.replicationManager = new CorfuReplicationManager(topology.getLocalNodeDescriptor(),
                    metadataManager, configManager.getServerContext().getPluginConfigFilePath(), runtime);
        }

        createSessions(true);
        outgoingSessions.forEach(session -> {
            ClusterDescriptor remoteClusterDescriptor = topology.getSinkClusters().get(session.getSinkClusterId());
            replicationManager.start(remoteClusterDescriptor, session, sessionToContextMap.get(session));
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
