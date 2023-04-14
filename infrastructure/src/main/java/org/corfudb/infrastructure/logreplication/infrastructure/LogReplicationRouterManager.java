package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceBaseRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationHandler;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkClientRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkServerRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceClientRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceServerRouter;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

import java.util.HashMap;
import java.util.Map;

/**
 * This class manages all the session routers
 */
@Slf4j
public class LogReplicationRouterManager {

    private final Map<LogReplicationSession, LogReplicationSourceBaseRouter> replicationSessionToRouterSource;
    private final Map<LogReplicationSession, LogReplicationSinkServerRouter> replicationSessionToRouterSink;

    @Setter
    private TopologyDescriptor topology;

    LogReplicationRouterManager(TopologyDescriptor topology) {
        this.topology = topology;
        this.replicationSessionToRouterSource = new HashMap<>();
        this.replicationSessionToRouterSink = new HashMap<>();
    }

    /**
     * Creates a source router if not already created:
     * A source-client router if cluster is connection starter else a source-server router
     */
    public LogReplicationSourceBaseRouter getOrCreateSourceRouter(LogReplicationSession session,
                                                                  Map<Class, AbstractServer> serverMap,
                                                                  boolean isConnectionStarter,
                                                                  LogReplicationRuntimeParameters params,
                                                                  CorfuReplicationManager replicationManager,
                                                                  ClusterDescriptor remote) {
        if (replicationSessionToRouterSource.containsKey(session)) {
            return replicationSessionToRouterSource.get(session);
        }
        LogReplicationSourceBaseRouter router;
        if (isConnectionStarter) {
            router = new LogReplicationSourceClientRouter(remote, params, replicationManager, session);
        } else {
            router = new LogReplicationSourceServerRouter(remote, params, replicationManager, session, serverMap);
        }
        replicationSessionToRouterSource.put(session, router);
        return router;
    }

    /**
     * Creates a sink router if not already created:
     * A sink-client router if cluster is connection starter else a sink-server router
     */
    public LogReplicationSinkServerRouter getOrCreateSinkRouter(LogReplicationSession session,
                                                                Map<Class, AbstractServer> serverMap,
                                                                boolean isConnectionStarter, String pluginFilePath,
                                                                long timeoutResponse) {
        if (replicationSessionToRouterSink.containsKey(session)) {
            return replicationSessionToRouterSink.get(session);
        }
        LogReplicationSinkServerRouter router;
        ClusterDescriptor remoteSource = topology.getRemoteSourceClusters().get(session.getSourceClusterId());
        if(isConnectionStarter) {
            LogReplicationSinkClientRouter sinkClientRouter = new LogReplicationSinkClientRouter(remoteSource,
                    pluginFilePath, timeoutResponse, session, serverMap);
            sinkClientRouter.addClient(new LogReplicationHandler(session));
            router = sinkClientRouter;
        } else {
            router = new LogReplicationSinkServerRouter(serverMap);
        }
        replicationSessionToRouterSink.put(session, router);
        return router;
    }

    /**
     * Stop Source router for a session
     * @param session
     */
    public void stopSourceRouter(LogReplicationSession session) {
        replicationSessionToRouterSource.get(session).stop();
        replicationSessionToRouterSource.remove(session);
    }

    /**
     * Stop Sink router for a session
     * @param session
     */
    public void stopSinkClientRouter(LogReplicationSession session) {
        // if Sink is connection starter, stop the router.
        if (replicationSessionToRouterSink.get(session) instanceof LogReplicationSinkClientRouter) {
            log.info("Stopping the SINK client router for session {}", session);
            ((LogReplicationSinkClientRouter) replicationSessionToRouterSink.get(session)).stop();
        }
    }

    public void clearRouterInfo(LogReplicationSession session) {
        this.replicationSessionToRouterSink.remove(session);
    }

}
