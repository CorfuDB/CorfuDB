package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationBaseSourceRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationHandler;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkClientRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkServerRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceClientRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceServerRouter;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationUpgradeManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class manages Log Replication for multiple remote (sink) clusters.
 */
@Slf4j
public class CorfuReplicationManager {

    private final Map<LogReplicationSession, CorfuLogReplicationRuntime> sessionRuntimeMap = new HashMap<>();

    private final NodeDescriptor localNodeDescriptor;

    private final CorfuRuntime corfuRuntime;

    private final LogReplicationMetadataManager metadataManager;

    private final String pluginFilePath;

    private final LogReplicationUpgradeManager upgradeManager;

    private final Map<LogReplicationSession, LogReplicationBaseSourceRouter> replicationSessionToRouterSource;
    private final Map<LogReplicationSession, LogReplicationSinkServerRouter> replicationSessionToRouterSink;

    private TopologyDescriptor topology;

    private final LogReplicationContext replicationContext;

    private final Map<LogReplicationSession, LogReplicationRuntimeParameters> replicationSessionToRuntimeParams;

    /**
     * Constructor
     */
    public CorfuReplicationManager(TopologyDescriptor topology,
                                   LogReplicationMetadataManager metadataManager,
                                   String pluginFilePath, CorfuRuntime corfuRuntime,
                                   LogReplicationUpgradeManager upgradeManager,
                                   LogReplicationContext replicationContext) {
        this.metadataManager = metadataManager;
        this.pluginFilePath = pluginFilePath;
        this.corfuRuntime = corfuRuntime;
        this.localNodeDescriptor = topology.getLocalNodeDescriptor();
        this.upgradeManager = upgradeManager;
        this.topology = topology;
        this.replicationContext = replicationContext;
        this.replicationSessionToRouterSource = new HashMap<>();
        this.replicationSessionToRouterSink = new HashMap<>();
        this.replicationSessionToRuntimeParams = new HashMap<>();
    }

    /**
     * Create router and initiate connection remote clusters
     * If local cluster is a source, creates a runtime and a sourceRouter
     * If local cluster is a sink, creates a router.
     */
    public void startConnection(LogReplicationSession session, Map<Class, AbstractServer> serverMap) {

        ClusterDescriptor remote;
        if (session.getSinkClusterId().equals(topology.getLocalClusterDescriptor().getClusterId())) {
            remote = topology.getAllClustersInTopology().get(session.getSourceClusterId());
        } else {
            remote = topology.getAllClustersInTopology().get(session.getSinkClusterId());
        }

        log.info("Starting connection to remote {} for session {} ", remote, session);
        if (!topology.getRemoteSinkClusters().isEmpty() &&
                topology.getRemoteSinkClusters().containsKey(remote.getClusterId())) {
            LogReplicationBaseSourceRouter router = getOrCreateSourceRouter(session, serverMap, true);
            try {
                IRetry.build(IntervalRetry.class, () -> {
                    try {
                        ((LogReplicationSourceClientRouter) router).connect();
                    } catch (Exception e) {
                        log.error("Failed to connect to remote cluster for session {}. Retry after 1 second. Exception {}.",
                                session, e);
                        throw new RetryNeededException();
                    }
                    return null;
                }).run();
            } catch (InterruptedException e) {
                log.error("Unrecoverable exception when attempting to connect to remote session.", e);
            }
        } else if (!topology.getRemoteSourceClusters().isEmpty() &&
                topology.getRemoteSourceClusters().containsKey(remote.getClusterId())) {
            LogReplicationSinkServerRouter router = getOrCreateSinkRouter(session, serverMap, true);
            try {
                IRetry.build(IntervalRetry.class, () -> {
                    try {
                        ((LogReplicationSinkClientRouter) router).connect();
                    } catch (Exception e) {
                        log.error("Failed to connect to remote cluster for session {}. Retry after 1 second.Exception {}.",
                                session, e);
                        throw new RetryNeededException();
                    }
                    return null;
                }).run();
            } catch (InterruptedException e) {
                log.error("Unrecoverable exception when attempting to connect to remote session.", e);
            }
        } else {
            log.warn("Not connecting to cluster {} as it is neither a source nor a sink to the current cluster", remote);
        }
    }

    /**
     * Creates a source router if not already created:
     * A source-client router if cluster is connection starter else a source-server router
     */
    public LogReplicationBaseSourceRouter getOrCreateSourceRouter(LogReplicationSession session,
                                                                  Map<Class, AbstractServer> serverMap, boolean isConnectionStarter) {
        if (replicationSessionToRouterSource.containsKey(session)) {
            return replicationSessionToRouterSource.get(session);
        }
        LogReplicationBaseSourceRouter router;
        ClusterDescriptor remoteSink = topology.getRemoteSinkClusters().get(session.getSinkClusterId());
        if (isConnectionStarter) {
            router = new LogReplicationSourceClientRouter(remoteSink,
                    localNodeDescriptor.getClusterId(), createRuntimeParams(remoteSink, session), this,
                    session);;
        } else {
            router = new LogReplicationSourceServerRouter(remoteSink,
                    localNodeDescriptor.getClusterId(), createRuntimeParams(remoteSink, session), this,
                    session, serverMap);;
        }
        replicationSessionToRouterSource.put(session, router);
        createRuntime(remoteSink, session);
        router.setRuntimeFSM(sessionRuntimeMap.get(session));

        return router;
    }

    /**
     * Creates a sink router if not already created:
     * A sink-client router if cluster is connection starter else a sink-server router
     */
    public LogReplicationSinkServerRouter getOrCreateSinkRouter(LogReplicationSession session,
                                                                Map<Class, AbstractServer> serverMap,
                                                                boolean isConnectionStarter) {
        if (replicationSessionToRouterSink.containsKey(session)) {
            log.info("Router was already created");
            return replicationSessionToRouterSink.get(session);
        }
        LogReplicationSinkServerRouter router;
        ClusterDescriptor remoteSource = topology.getRemoteSourceClusters().get(session.getSourceClusterId());
        if(isConnectionStarter) {
            LogReplicationSinkClientRouter sinkClientRouter = new LogReplicationSinkClientRouter(remoteSource,
                    localNodeDescriptor.getClusterId(), pluginFilePath, corfuRuntime.getParameters()
                    .getRequestTimeout().toMillis(), session, serverMap);
            sinkClientRouter.addClient(new LogReplicationHandler(session));
            router = sinkClientRouter;
        } else {
            router = new LogReplicationSinkServerRouter(serverMap);
        }
        replicationSessionToRouterSink.put(session, router);
        return router;
    }

    /**
     * Create Log Replication Runtime for a session, if not already created.
     */
    public void createRuntime(ClusterDescriptor remote, LogReplicationSession replicationSession) {
        try {
            CorfuLogReplicationRuntime replicationRuntime;
            if (!sessionRuntimeMap.containsKey(replicationSession)) {
                log.info("Creating Log Replication Runtime for session {}", replicationSession);
                LogReplicationRuntimeParameters parameters;
                // parameters is null when the cluster is not the connection starter.
                if (replicationSessionToRuntimeParams.isEmpty() || replicationSessionToRuntimeParams.get(replicationSession) == null) {
                    parameters = createRuntimeParams(remote, replicationSession);
                } else {
                    parameters = replicationSessionToRuntimeParams.get(replicationSession);
                }
                replicationRuntime = new CorfuLogReplicationRuntime(parameters,
                        metadataManager, upgradeManager, replicationSession, replicationContext,
                        replicationSessionToRouterSource.get(replicationSession));
                sessionRuntimeMap.put(replicationSession, replicationRuntime);
            } else {
                log.warn("Log Replication Runtime to remote session {}, already exists. Skipping init.",
                        replicationSession);
                return;
            }
        } catch (Exception e) {
            log.error("Caught exception, stop log replication runtime to {}", replicationSession, e);
            stopLogReplicationRuntime(replicationSession);
        }
    }

    /**
     * Start log replication.
     * For the connection initiator cluster, this is called when the connection is established.
     */
    public void startLogReplicationRuntime(LogReplicationSession replicationSession) {
        CorfuLogReplicationRuntime replicationRuntime;
        synchronized (this) {
            replicationRuntime = sessionRuntimeMap.get(replicationSession);
        }
        replicationRuntime.start();
    }

    // TODO (V2): we might think of unifying the info in ClusterDescriptor into session (all nodes host+port)
    private LogReplicationRuntimeParameters createRuntimeParams(ClusterDescriptor remoteCluster, LogReplicationSession session) {
        LogReplicationRuntimeParameters parameters = LogReplicationRuntimeParameters.builder()
                .localCorfuEndpoint(replicationContext.getLocalCorfuEndpoint())
                .remoteClusterDescriptor(remoteCluster)
                .localClusterId(localNodeDescriptor.getClusterId())
                .pluginFilePath(pluginFilePath)
                .topologyConfigId(topology.getTopologyConfigId())
                .keyStore(corfuRuntime.getParameters().getKeyStore())
                .tlsEnabled(corfuRuntime.getParameters().isTlsEnabled())
                .ksPasswordFile(corfuRuntime.getParameters().getKsPasswordFile())
                .trustStore(corfuRuntime.getParameters().getTrustStore())
                .tsPasswordFile(corfuRuntime.getParameters().getTsPasswordFile())
                .maxWriteSize(corfuRuntime.getParameters().getMaxWriteSize())
                .build();

        replicationSessionToRuntimeParams.put(session, parameters);

        return parameters;
    }


    /**
     * Start log replication by instantiating a runtime for each session
     */


    /**
     * Stop log replication for all sessions
     */
    public void stop() {
        sessionRuntimeMap.keySet().forEach(session -> {
            log.info("Stop log replication runtime to remote cluster id={}", session.getSinkClusterId());
            stopLogReplicationRuntime(session);
        });
        sessionRuntimeMap.clear();
    }

    /**
     * Stop log replication for specific sessions
     *
     * @param sessions
     */
    public void stop(Set<LogReplicationSession> sessions) {
        sessions.forEach(session -> {
            stopLogReplicationRuntime(session);
            sessionRuntimeMap.remove(session);

        });
    }

    private void stopLogReplicationRuntime(LogReplicationSession session) {
        CorfuLogReplicationRuntime logReplicationRuntime = sessionRuntimeMap.get(session);
        if (logReplicationRuntime != null) {
            try {
                log.info("Stop log replication runtime for session {}", session);
                logReplicationRuntime.stop();
                replicationSessionToRouterSource.get(session).stop();
                replicationSessionToRouterSource.remove(session);
                replicationSessionToRuntimeParams.remove(session);

            } catch(Exception e) {
                log.warn("Failed to stop log replication runtime to remote cluster id={}", session.getSinkClusterId());
            }
        } else {
            log.warn("Runtime not found for session {}", session);
        }
    }

    public void updateTopology(TopologyDescriptor newTopology) {
        this.topology = newTopology;
    }

    /**
     * Remove router information for closed sessions
     * @param sessions
     */
    public void removeSinkRouterInfo(Set<LogReplicationSession> sessions) {
        sessions.forEach(session -> this.replicationSessionToRouterSink.remove(session));
    }


    public void refreshRuntime(LogReplicationSession session, ClusterDescriptor cluster, long topologyConfigId) {
        // The connection id or other transportation plugin's info could've changed for existing Sink clusters,
        // updating the routers will re-establish the connection to the correct endpoints/nodes
        sessionRuntimeMap.get(session).refresh(cluster, topologyConfigId);
    }

    /**
     * Stop the current log replication event and start a full snapshot sync for the given session.
     */
    public void enforceSnapshotSync(DiscoveryServiceEvent event) {
        CorfuLogReplicationRuntime runtime = sessionRuntimeMap.get(event.getSession());
        if (runtime == null) {
            log.warn("Failed to enforce snapshot sync for session {}",
                event.getSession());
        } else {
            log.info("Enforce snapshot sync for remote session {}", event.getSession());
            runtime.getSourceManager().stopLogReplication();
            runtime.getSourceManager().startForcedSnapshotSync(event.getEventId());
        }
    }
}
