package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationUpgradeManager;
import org.corfudb.runtime.CorfuRuntime;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeStateType.WAITING_FOR_CONNECTIVITY;

/**
 * This class manages Log Replication for multiple remote (sink) clusters.
 */
@Slf4j
public class CorfuReplicationManager {

    @Getter
    private final Map<LogReplicationSession, CorfuLogReplicationRuntime> sessionRuntimeMap = new ConcurrentHashMap<>();

    private final NodeDescriptor localNodeDescriptor;

    private final CorfuRuntime corfuRuntime;

    private final LogReplicationMetadataManager metadataManager;

    private final String pluginFilePath;

    private final LogReplicationUpgradeManager upgradeManager;

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
        this.replicationSessionToRuntimeParams = new HashMap<>();
    }

    /**
     * Create Log Replication Runtime for a session, if not already created.
     */
    public void createRuntime(ClusterDescriptor remote, LogReplicationSession replicationSession,
                              LogReplicationClientServerRouter router) {
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
                        metadataManager, upgradeManager, replicationSession, replicationContext, router);
                sessionRuntimeMap.put(replicationSession, replicationRuntime);
                router.addRuntimeFSM(replicationSession, replicationRuntime);
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
            // possible to get the force snapshot sync for a session before the FSM is started. In that case, block
            // until FSM gets a start notification from the router
            try {
                runtime.getAwaitRuntimeFsmStart().await();
            } catch (InterruptedException e) {
                log.error("The thread was interrupted while waiting for FSM to start. Skip snapshot sync ", e);
                return;
            }
            runtime.getSourceManager().stopLogReplication();
            runtime.getSourceManager().startForcedSnapshotSync(event.getEventId());
        }
    }
}
