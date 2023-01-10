package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationHandler;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkClientRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkServerRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceClientRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceRouterHelper;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceServerRouter;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class manages Log Replication for multiple remote (sink) clusters.
 */
@Slf4j
public class CorfuReplicationManager {

    // Keep map of remote session and the associated log replication runtime (an abstract client to that cluster)
    @Getter
    private final Map<ReplicationSession, CorfuLogReplicationRuntime> remoteSesionToRuntime = new HashMap<>();

    @Setter
    private LogReplicationContext context;

    private final NodeDescriptor localNodeDescriptor;

    private final CorfuRuntime corfuRuntime;

    private final Map<ReplicationSession, LogReplicationMetadataManager> metadataManagerMap;

    private final String pluginFilePath;

    private final LogReplicationConfigManager replicationConfigManager;

    private final Map<ReplicationSession, LogReplicationSourceRouterHelper> replicationSessionToRouterSource;
    private final Map<ReplicationSession, LogReplicationSinkServerRouter> replicationSessionToRouterSink;
    private final Map<ReplicationSession, LogReplicationRuntimeParameters> replicationSessionToRuntimeParams;

    /**
     * Constructor
     */
    public CorfuReplicationManager(LogReplicationContext context, NodeDescriptor localNodeDescriptor,
                                   Map<ReplicationSession, LogReplicationMetadataManager> metadataManagerMap,
                                   String pluginFilePath, CorfuRuntime corfuRuntime,
                                   LogReplicationConfigManager replicationConfigManager) {
        this.context = context;
        this.metadataManagerMap = metadataManagerMap;
        this.pluginFilePath = pluginFilePath;
        this.corfuRuntime = corfuRuntime;
        this.localNodeDescriptor = localNodeDescriptor;
        this.replicationConfigManager = replicationConfigManager;
        this.replicationSessionToRouterSource = new HashMap<>();
        this.replicationSessionToRouterSink = new HashMap<>();
        this.replicationSessionToRuntimeParams = new HashMap<>();
    }

    /**
     * Called when the local cluster is a connection initiator.
     * If local cluster is a source, creates a runtime and a sourceRouter
     * If local cluster is a sink, creates a router and starts the sink-server node.
     */
    public void startConnection(Set<ClusterDescriptor> remoteClusters,
                                Map<String, Set<ReplicationSession>> remoteClusterIdToReplicationSession,
                                Map<Class, AbstractServer> serverMap) {

        for(ClusterDescriptor remote : remoteClusters) {
            Set<ReplicationSession> sessions = remoteClusterIdToReplicationSession.get(remote.getClusterId());
            log.info("Starting connection to remote {} for session {} ", remote, sessions);
            if(sessions.isEmpty()) {
                continue;
            }

            for(ReplicationSession session : sessions) {
                if (!context.getTopology().getRemoteSinkClusters().isEmpty() &&
                        context.getTopology().getRemoteSinkClusters().containsKey(remote.getClusterId())) {
                    LogReplicationSourceRouterHelper router = getOrCreateSourceRouter(remote,session, serverMap, true);
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
                } else if (!this.context.getTopology().getRemoteSourceClusters().isEmpty() &&
                        this.context.getTopology().getRemoteSourceClusters().containsKey(remote.getClusterId())) {
                    LogReplicationSinkServerRouter router = getOrCreateSinkRouter(remote,session, serverMap, true);
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
                    continue;
                }
            }

        }
    }

    /**
     * Creates a source router if not already created:
     * A source-client router if cluster is connection starter else a source-server router
     */
    public LogReplicationSourceRouterHelper getOrCreateSourceRouter(ClusterDescriptor remote, ReplicationSession session,
                                                                    Map<Class, AbstractServer> serverMap, boolean isConnectionStarter) {
        if (replicationSessionToRouterSource.containsKey(session)) {
            return replicationSessionToRouterSource.get(session);
        }
        LogReplicationSourceRouterHelper router;
        if (isConnectionStarter) {
            router = new LogReplicationSourceClientRouter(remote,
                    localNodeDescriptor.getClusterId(), createRuntimeParams(remote, session), this,
                    session);;
        } else {
            router = new LogReplicationSourceServerRouter(remote,
                    localNodeDescriptor.getClusterId(), createRuntimeParams(remote, session), this,
                    session, serverMap);;
        }
        replicationSessionToRouterSource.put(session, router);
        createRuntime(remote, session);
        router.setRuntimeFSM(remoteSesionToRuntime.get(session));

        return router;
    }

    /**
     * Creates a sink router if not already created:
     * A sink-client router if cluster is connection starter else a sink-server router
     */
    public LogReplicationSinkServerRouter getOrCreateSinkRouter(ClusterDescriptor remote, ReplicationSession session,
                                                                Map<Class, AbstractServer> serverMap, boolean isConnectionStarter) {
        if (replicationSessionToRouterSink.containsKey(session)) {
            return replicationSessionToRouterSink.get(session);
        }
        LogReplicationSinkServerRouter router;
        if(isConnectionStarter) {
            LogReplicationSinkClientRouter sinkClientRouter = new LogReplicationSinkClientRouter(remote,
                    localNodeDescriptor.getClusterId(), pluginFilePath, corfuRuntime.getParameters()
                    .getRequestTimeout().toMillis(), session, serverMap);
            sinkClientRouter.addClient(new LogReplicationHandler(session));
            router = sinkClientRouter;
        } else {
            LogReplicationSinkServerRouter sinkServerRouter = new LogReplicationSinkServerRouter(serverMap);
            router = sinkServerRouter;
        }
        replicationSessionToRouterSink.put(session, router);
        return router;
    }

    /**
     * Create Log Replication Runtime for a session, if not already created.
     */
    public void createRuntime(ClusterDescriptor remote, ReplicationSession replicationSession) {
        try {
            CorfuLogReplicationRuntime replicationRuntime;
                if (!remoteSesionToRuntime.containsKey(replicationSession)) {
                    log.info("Starting Log Replication Runtime for session {}", replicationSession);
                    LogReplicationRuntimeParameters parameters;
                    // parameters is null when the cluster is not the connection starter.
                    if (replicationSessionToRuntimeParams.isEmpty() || replicationSessionToRuntimeParams.get(replicationSession) == null) {
                        parameters = createRuntimeParams(remote, replicationSession);
                    } else {
                        parameters = replicationSessionToRuntimeParams.get(replicationSession);
                    }
                    replicationRuntime = new CorfuLogReplicationRuntime(parameters,
                            metadataManagerMap.get(replicationSession), replicationConfigManager, replicationSession,
                            replicationSessionToRouterSource.get(replicationSession));
                    remoteSesionToRuntime.put(replicationSession, replicationRuntime);
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
    public void startRuntime(ReplicationSession replicationSession) {
        CorfuLogReplicationRuntime replicationRuntime;
        synchronized (this) {
            replicationRuntime = remoteSesionToRuntime.get(replicationSession);
        }
        replicationRuntime.start();
    }

    private LogReplicationRuntimeParameters createRuntimeParams(ClusterDescriptor remoteCluster, ReplicationSession session) {
        LogReplicationRuntimeParameters parameters = LogReplicationRuntimeParameters.builder()
                .localCorfuEndpoint(context.getLocalCorfuEndpoint())
                .remoteClusterDescriptor(remoteCluster)
                .localClusterId(localNodeDescriptor.getClusterId())
                .replicationConfig(context.getConfig())
                .pluginFilePath(pluginFilePath)
                .topologyConfigId(context.getTopology().getTopologyConfigId())
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
     * Stop log replication for all the sink sites
     */
    public void stop() {
        //Stop the source component
        remoteSesionToRuntime.forEach((session, runtime) -> {
            try {
                log.info("Stop log replication runtime and source router for session={}", session);
                runtime.stop();
                replicationSessionToRouterSource.get(session).stop();
            } catch (Exception e) {
                log.warn("Failed to stop log replication runtime for session={}", session);
            }
        });
        remoteSesionToRuntime.clear();
        replicationSessionToRouterSource.clear();
        replicationSessionToRuntimeParams.clear();

        //Stop the sink component
        replicationSessionToRouterSink.values().stream().forEach(router -> ((LogReplicationSinkClientRouter)router).stop());
    }

    /**
     * Stop Log Replication to a specific Sink session
     */
    private void stopLogReplicationRuntime(ReplicationSession replicationSession) {
        CorfuLogReplicationRuntime logReplicationRuntime = remoteSesionToRuntime.get(replicationSession);
        if (logReplicationRuntime != null) {
            log.info("Stop log replication runtime and source router for remote session {}", replicationSession);
            logReplicationRuntime.stop();
            replicationSessionToRouterSource.get(replicationSession).stop();

            remoteSesionToRuntime.remove(replicationSession);
            replicationSessionToRouterSource.remove(replicationSession);
            replicationSessionToRuntimeParams.remove(replicationSession);
        } else {
            log.warn("Runtime not found for remote session {}", replicationSession);
        }
    }

    public void clearSessionToSinkRouterMap() {
        this.replicationSessionToRouterSink.clear();
    }

    /**
     * Update Log Replication Runtime config id.
     */
    private void updateRuntimeConfigId(TopologyDescriptor newConfig) {
        remoteSesionToRuntime.values().forEach(runtime -> runtime.updateFSMConfigId(newConfig));
    }

    /**
     * The notification of adding/removing remote clusters with/without topology configId change.
     *
     * @param newConfig should have the same topologyConfigId as the current config
     * @param remoteSinkClustersToAdd the new sink clusters to be added
     * @param remoteSourceClusterToAdd the new source clusters to be added
     * @param remoteClustersToRemove sink clusters which are not found in the new topology
     * @param intersection Sink clusters found in both old and new topologies
     */
    public void processRemoteClusterChange(TopologyDescriptor newConfig, Set<String> remoteSinkClustersToAdd,
                                           Set<String> remoteSourceClusterToAdd, Set<String> remoteClustersToRemove,
                                           Set<String> intersection, Map<String, ClusterDescriptor> connectionEndsIdToDescriptor,
                                           Map<String, Set<ReplicationSession>> remoteIdToReplicationSession,
                                           Map<Class, AbstractServer> serverMap) {

        long oldTopologyConfigId = context.getTopology().getTopologyConfigId();
        context.setTopology(newConfig);

        // Get all subscribers
        Set<ReplicationSubscriber> subscribers = context.getConfig().getReplicationSubscriberToStreamsMap().keySet();

        // Remove Sinks that are not in the new config
        for (String clusterId : remoteClustersToRemove) {
            for (ReplicationSubscriber subscriber : subscribers) {
                stopLogReplicationRuntime(new ReplicationSession(clusterId, localNodeDescriptor.getClusterId(), subscriber));
            }
        }

        // Add the new clusters to LR
        Set<ClusterDescriptor> newConnectionToStart = new HashSet<>();
        Set<String> remoteClustersToAdd = new HashSet<>();
        remoteClustersToAdd.addAll(remoteSinkClustersToAdd);
        remoteClustersToAdd.addAll(remoteSourceClusterToAdd);

        for (String clusterId : remoteClustersToAdd) {
            // When the local cluster is the connection initiator to the newly added sink, collect the remoteSinks to start connections
            if(connectionEndsIdToDescriptor.containsKey(clusterId)) {
                newConnectionToStart.add(connectionEndsIdToDescriptor.get(clusterId));
            } else {
                if (remoteSinkClustersToAdd.contains(clusterId)) {
                    remoteIdToReplicationSession.get(clusterId).stream().forEach(session -> {
                        getOrCreateSourceRouter(connectionEndsIdToDescriptor.get(clusterId), session, serverMap, false);
                        startRuntime(session);
                    });
                } else {
                    remoteIdToReplicationSession.get(clusterId).stream().forEach(session -> {
                        getOrCreateSinkRouter(connectionEndsIdToDescriptor.get(clusterId), session, serverMap, false);
                    });
                }
            }
        }
        if (!newConnectionToStart.isEmpty()) {
            startConnection(newConnectionToStart, remoteIdToReplicationSession, serverMap);
        }

        // The connection id or other transportation plugin's info could've changed for existing Sink clusters,
        // updating the routers will re-establish the connection to the correct endpoints/nodes
        for (String clusterId : intersection) {
            ClusterDescriptor clusterInfo = newConfig.getRemoteSinkClusters().get(clusterId);
            for (ReplicationSubscriber subscriber : subscribers) {
                remoteSesionToRuntime.get(new ReplicationSession(clusterId, localNodeDescriptor.getClusterId(), subscriber))
                    .updateRouterClusterDescriptor(clusterInfo);
            }
        }

        if (oldTopologyConfigId != newConfig.getTopologyConfigId()) {
            updateRuntimeConfigId(newConfig);
        }
    }

    /**
     * Stop the current log replication event and start a full snapshot sync for the given remote cluster.
     */
    public void enforceSnapshotSync(DiscoveryServiceEvent event) {
        CorfuLogReplicationRuntime sinkRuntime = remoteSesionToRuntime.get(
            ReplicationSession.getDefaultReplicationSessionForCluster(event.getRemoteClusterInfo().getClusterId(), localNodeDescriptor.getClusterId()));
        if (sinkRuntime == null) {
            log.warn("Failed to start enforceSnapshotSync for cluster {} as no runtime to it was found",
                event.getRemoteClusterInfo().getClusterId());
        } else {
            log.info("EnforceSnapshotSync for cluster {}", sinkRuntime.getRemoteClusterId());
            sinkRuntime.getSourceManager().stopLogReplication();
            sinkRuntime.getSourceManager().startForcedSnapshotSync(event.getEventId());
        }
    }
}
