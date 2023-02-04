package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
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
import java.util.concurrent.ConcurrentHashMap;


/**
 * This class manages Log Replication for multiple remote (sink) clusters.
 */
@Slf4j
public class CorfuReplicationManager {

    private final Map<LogReplicationSession, CorfuLogReplicationRuntime> sessionRuntimeMap = new HashMap<>();

    private final NodeDescriptor localNodeDescriptor;

    @Getter
    private final CorfuRuntime corfuRuntime;

    private final LogReplicationMetadataManager metadataManager;

    private TopologyDescriptor topology;

    private final LogReplicationContext replicationContext;

    private final LogReplicationUpgradeManager upgradeManager;

    /**
     * Constructor
     */
    public CorfuReplicationManager(NodeDescriptor localNodeDescriptor,
                                   LogReplicationMetadataManager metadataManager,
                                   String pluginFilePath, CorfuRuntime corfuRuntime, LogReplicationUpgradeManager upgradeManager) {
        this.metadataManager = metadataManager;
        this.pluginFilePath = pluginFilePath;
        this.corfuRuntime = corfuRuntime;
        this.localNodeDescriptor = localNodeDescriptor;
        this.upgradeManager = upgradeManager;
    }

    /**
     * Start log replication by instantiating a runtime for each session
     */
    public void start(ClusterDescriptor remoteCluster, LogReplicationSession session,
                      LogReplicationContext replicationContext) {
        try {
            // TODO (V2): we might think of unifying the info in ClusterDescriptor into session (all nodes host+port)
            startLogReplicationRuntime(remoteCluster, session, replicationContext);
        } catch (Exception e) {
            log.error("Failed to start log replication runtime for session={}", session);
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

    private void startLogReplicationRuntime(ClusterDescriptor remoteClusterDescriptor,
                                            LogReplicationSession session, LogReplicationContext replicationContext) {
        try {
            if (!sessionRuntimeMap.containsKey(session)) {
                log.info("Starting Log Replication Runtime for session {}", session);
                connect(remoteClusterDescriptor, session, replicationContext);
            } else {
                log.warn("Log Replication Runtime for session {}, already exists. Skip.", session);
            }
        } catch (Exception e) {
            log.error("Caught exception, stop log replication runtime to {}", session, e);
            stopLogReplicationRuntime(session);
            sessionRuntimeMap.remove(session);
        }
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


    /**
     * Stop log replication for specific sessions
     *
     * @param sessions
     */
    private void connect(ClusterDescriptor remoteCluster, LogReplicationSession session, LogReplicationContext context)
            throws InterruptedException {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    LogReplicationRuntimeParameters parameters = LogReplicationRuntimeParameters.builder()
                            .session(session)
                            .localCorfuEndpoint(context.getLocalCorfuEndpoint())
                            .remoteClusterDescriptor(remoteCluster)
                            .localClusterId(localNodeDescriptor.getClusterId())
                            .pluginFilePath(pluginFilePath)
                            .topologyConfigId(context.getTopologyConfigId())
                            .keyStore(corfuRuntime.getParameters().getKeyStore())
                            .tlsEnabled(corfuRuntime.getParameters().isTlsEnabled())
                            .ksPasswordFile(corfuRuntime.getParameters().getKsPasswordFile())
                            .trustStore(corfuRuntime.getParameters().getTrustStore())
                            .tsPasswordFile(corfuRuntime.getParameters().getTsPasswordFile())
                            .maxWriteSize(corfuRuntime.getParameters().getMaxWriteSize())
                            .build();
                    CorfuLogReplicationRuntime replicationRuntime = new CorfuLogReplicationRuntime(parameters,
                        metadataManager, upgradeManager, session, context);
                    replicationRuntime.start();
                    sessionRuntimeMap.put(session, replicationRuntime);
                } catch (Exception e) {
                    log.error("Failed to connect to remote cluster for session {}. Retry after 1 second.", session, e);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to connect to remote session.", e);
            throw e;
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
