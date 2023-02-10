//package org.corfudb.infrastructure.logreplication.runtime;
//
//import lombok.Getter;
//import lombok.extern.slf4j.Slf4j;
//import org.corfudb.infrastructure.AbstractServer;
//import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
//import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
//import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationManager;
//import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
//import org.corfudb.runtime.CorfuRuntime;
//import org.corfudb.runtime.LogReplication.LogReplicationSession;
//import org.corfudb.util.retry.IRetry;
//import org.corfudb.util.retry.IntervalRetry;
//import org.corfudb.util.retry.RetryNeededException;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Set;
//
//@Slf4j
//public class LogReplicationRouterManager {
//
//    // Shama : write comments
//    @Getter
//    private final Map<LogReplicationSession, LogReplicationBaseSourceRouter> replicationSessionToRouterSource;
//
//    @Getter
//    private final Map<LogReplicationSession, LogReplicationSinkServerRouter> replicationSessionToRouterSink;
//
//    private TopologyDescriptor topology;
//
//    private final CorfuRuntime corfuRuntime;
//
//    private final String pluginFilePath;
//
//    private final String localClusterId;
//
////    private final CorfuReplicationManager replicationManager;
//
//    public LogReplicationRouterManager(TopologyDescriptor topology, CorfuRuntime corfuRuntime, String pluginFilePath,
//                                       CorfuReplicationManager replicationManager) {
//        this.topology = topology;
//        this.corfuRuntime = corfuRuntime;
//        this.pluginFilePath = pluginFilePath;
////        this.replicationManager = replicationManager;
//        this.localClusterId = topology.getLocalClusterDescriptor().getClusterId();
//        this.replicationSessionToRouterSource = new HashMap<>();
//        this.replicationSessionToRouterSink = new HashMap<>();
//    }
//
//    private boolean isConnectionInitiator(String remoteClusterId) {
//        return topology.getRemoteClusterEndpoints().containsKey(remoteClusterId);
//    }
//
//    public void createRouter(Set<LogReplicationSession> sessions, boolean isSource) {
//        sessions.forEach(session -> createRouter(session, isSource, null));
//    }
//
//    public void createRouter(LogReplicationSession session, boolean isSource, LogReplicationRuntimeParameters params) {
//        // check with endpoint
//        String remoteClusterId;
//        if (session.getSinkClusterId().equals(topology.getLocalClusterDescriptor().getClusterId())) {
//            remoteClusterId = session.getSourceClusterId();
//        } else {
//            remoteClusterId = session.getSinkClusterId();
//        }
//
//        if (topology.getRemoteClusterEndpoints().containsKey(remoteClusterId)) {
////            startConnection();
//        } else if (isSource){
////            getOrCreateSourceRouter();
//        } else {
////            getOrCreateSinkRouter();
//        }
//    }
//
//    public void createSourceRouter(LogReplicationSession session, LogReplicationRuntimeParameters params) {
//        String remoteClusterId = session.getSinkClusterId();
//        if (isConnectionInitiator(remoteClusterId)) {
//            getOrCreateSourceRouter(topology.getRemoteSinkClusters().get(remoteClusterId), session)
//        }
//    }
//
//    /**
//     * Called when the local cluster is a connection initiator.
//     * If local cluster is a source, creates a runtime and a sourceRouter
//     * If local cluster is a sink, creates a router and starts the sink-server node.
//     */
//    // Shama change this to 1 session. => only connection init.
//    private void startConnection(Set<ClusterDescriptor> remoteClusters,
//                                Map<String, Set<LogReplicationSession>> remoteClusterIdToReplicationSession,
//                                Map<Class, AbstractServer> serverMap) {
//
//        for (ClusterDescriptor remote : remoteClusters) {
//            Set<LogReplicationSession> sessions = remoteClusterIdToReplicationSession.get(remote.getClusterId());
//            log.info("Starting connection to remote {} for session {} ", remote, sessions);
//            for (LogReplicationSession session : sessions) {
//                if (!topology.getRemoteSinkClusters().isEmpty() &&
//                        topology.getRemoteSinkClusters().containsKey(remote.getClusterId())) {
//                    LogReplicationBaseSourceRouter router = getOrCreateSourceRouter(remote, session, serverMap, true);
//                    try {
//                        IRetry.build(IntervalRetry.class, () -> {
//                            try {
//                                ((LogReplicationSourceClientRouter) router).connect();
//                            } catch (Exception e) {
//                                log.error("Failed to connect to remote cluster for session {}. Retry after 1 second. Exception {}.",
//                                        session, e);
//                                throw new RetryNeededException();
//                            }
//                            return null;
//                        }).run();
//                    } catch (InterruptedException e) {
//                        log.error("Unrecoverable exception when attempting to connect to remote session.", e);
//                    }
//                } else if (!topology.getRemoteSourceClusters().isEmpty() &&
//                        topology.getRemoteSourceClusters().containsKey(remote.getClusterId())) {
//                    LogReplicationSinkServerRouter router = getOrCreateSinkRouter(remote, session, serverMap, true);
//                    try {
//                        IRetry.build(IntervalRetry.class, () -> {
//                            try {
//                                ((LogReplicationSinkClientRouter) router).connect();
//                            } catch (Exception e) {
//                                log.error("Failed to connect to remote cluster for session {}. Retry after 1 second.Exception {}.",
//                                        session, e);
//                                throw new RetryNeededException();
//                            }
//                            return null;
//                        }).run();
//                    } catch (InterruptedException e) {
//                        log.error("Unrecoverable exception when attempting to connect to remote session.", e);
//                    }
//                } else {
//                    log.warn("Not connecting to cluster {} as it is neither a source nor a sink to the current cluster", remote);
//                    continue;
//                }
//            }
//
//        }
//    }
//
//    /**
//     * Creates a source router if not already created:
//     * A source-client router if cluster is connection starter else a source-server router
//     */
//    public LogReplicationBaseSourceRouter getOrCreateSourceRouter(ClusterDescriptor remote, LogReplicationSession session,
//                                                                  Map<Class, AbstractServer> serverMap,
//                                                                  LogReplicationRuntimeParameters params) {
//        if (replicationSessionToRouterSource.containsKey(session)) {
//            return replicationSessionToRouterSource.get(session);
//        }
//        LogReplicationBaseSourceRouter router = router = new LogReplicationSourceServerRouter(remote,
//                topology.getLocalClusterDescriptor().getClusterId(), params,
//                session, serverMap);
////        if (isConnectionStarter) {
////            router = new LogReplicationSourceClientRouter(remote,
////                    topology.getLocalClusterDescriptor().getClusterId(), createRuntimeParams(remote, session), this,
////                    session);
////        } else {
////            router = new LogReplicationSourceServerRouter(remote,
////                    topology.getLocalClusterDescriptor().getClusterId(), createRuntimeParams(remote, session), this,
////                    session, serverMap);
////        }
//        replicationSessionToRouterSource.put(session, router);
//
//        return router;
//    }
//
//    /**
//     * Creates a source router if not already created:
//     * A source-client router if cluster is connection starter else a source-server router
//     */
//    public LogReplicationBaseSourceRouter getOrCreateSourceRouter(ClusterDescriptor remote, LogReplicationSession session,
//                                                                  Map<Class, AbstractServer> serverMap, boolean isConnectionStarter) {
//        if (replicationSessionToRouterSource.containsKey(session)) {
//            return replicationSessionToRouterSource.get(session);
//        }
//        LogReplicationBaseSourceRouter router;
//        if (isConnectionStarter) {
//            router = new LogReplicationSourceClientRouter(remote,
//                    topology.getLocalClusterDescriptor().getClusterId(), createRuntimeParams(remote, session), this,
//                    session);;
//        } else {
//            router = new LogReplicationSourceServerRouter(remote,
//                    topology.getLocalClusterDescriptor().getClusterId(), createRuntimeParams(remote, session), this,
//                    session, serverMap);;
//        }
//        replicationSessionToRouterSource.put(session, router);
//        createRuntime(remote, session);
//        router.setRuntimeFSM(remoteSesionToRuntime.get(session));
//
//        return router;
//    }
//
//    /**
//     * Creates a sink router if not already created:
//     * A sink-client router if cluster is connection starter else a sink-server router
//     */
//    public LogReplicationSinkServerRouter getOrCreateSinkRouter(ClusterDescriptor remote, LogReplicationSession session,
//                                                                Map<Class, AbstractServer> serverMap, boolean isConnectionStarter) {
//        if (replicationSessionToRouterSink.containsKey(session)) {
//            return replicationSessionToRouterSink.get(session);
//        }
//        LogReplicationSinkServerRouter router;
//        if(isConnectionStarter) {
//            LogReplicationSinkClientRouter sinkClientRouter = new LogReplicationSinkClientRouter(remote,
//                    topology.getLocalClusterDescriptor().getClusterId(), pluginFilePath, corfuRuntime.getParameters()
//                    .getRequestTimeout().toMillis(), session, serverMap);
//            sinkClientRouter.addClient(new LogReplicationHandler(session));
//            router = sinkClientRouter;
//        } else {
//            LogReplicationSinkServerRouter sinkServerRouter = new LogReplicationSinkServerRouter(serverMap);
//            router = sinkServerRouter;
//        }
//        replicationSessionToRouterSink.put(session, router);
//        return router;
//    }
//
//    public void stop(LogReplicationSession session) {
//
//    }
//
//}
