package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This class manages Log Replication for multiple remote (sink) clusters.
 *
 * The access to all the methods, except to sessionRuntimeMap, is single threaded.
 * Multiple threads (CorfuReplicationManager and the LogReplicationClientServerRouter) read sessionRuntimeMap to access the
 * runtimeFSM for a session.
 */
@Slf4j
public class CorfuReplicationManager {

    @Getter
    private final Map<LogReplicationSession, CorfuLogReplicationRuntime> sessionRuntimeMap = new ConcurrentHashMap<>();

    private final LogReplicationMetadataManager metadataManager;

    @Getter
    private final LogReplicationContext replicationContext;

    /**
     * Constructor
     */
    public CorfuReplicationManager(LogReplicationMetadataManager metadataManager,
                                   LogReplicationContext replicationContext) {
        this.metadataManager = metadataManager;
        this.replicationContext = replicationContext;
    }

    /**
     * Create Log Replication Runtime for a session, if not already created.
     */
    public void createAndStartRuntime(ClusterDescriptor remote, LogReplicationSession replicationSession,
                                      LogReplicationClientServerRouter router) {
        try {
            CorfuLogReplicationRuntime replicationRuntime;
            if (!sessionRuntimeMap.containsKey(replicationSession)) {
                log.info("Creating Log Replication Runtime for session {}", replicationSession);
                replicationRuntime = new CorfuLogReplicationRuntime(metadataManager, replicationSession, replicationContext, router);
                sessionRuntimeMap.put(replicationSession, replicationRuntime);
                router.addRuntimeFSM(replicationSession, replicationRuntime);
                replicationRuntime.start();
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

            } catch(Exception e) {
                log.warn("Failed to stop log replication runtime to remote cluster id={}", session.getSinkClusterId());
            }
        } else {
            log.warn("Runtime not found for session {}", session);
        }
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
