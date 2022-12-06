package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationSubscriber;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationSession;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
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

    @Setter
    private LogReplicationContext context;

    private final NodeDescriptor localNodeDescriptor;

    private final CorfuRuntime corfuRuntime;

    private final LogReplicationMetadataManager metadataManager;

    private final String pluginFilePath;

    private final LogReplicationConfigManager replicationConfigManager;

    /**
     * Constructor
     */
    public CorfuReplicationManager(LogReplicationContext context, NodeDescriptor localNodeDescriptor,
                                   LogReplicationMetadataManager metadataManager,
                                   String pluginFilePath, CorfuRuntime corfuRuntime,
                                   LogReplicationConfigManager replicationConfigManager) {
        this.context = context;
        this.metadataManager = metadataManager;
        this.pluginFilePath = pluginFilePath;
        this.corfuRuntime = corfuRuntime;
        this.localNodeDescriptor = localNodeDescriptor;
        this.replicationConfigManager = replicationConfigManager;
    }

    /**
     * Start Log Replication Manager, this will initiate a runtime against
     * each Sink session(cluster + replication model + client), to further start log replication.
     */
    public void start() {
        for (ClusterDescriptor remoteCluster : context.getTopology().getSinkClusters().values()) {
            for (LogReplicationSession session : context.getConfig().getSessionToStreamsMap().keySet()) {
                try {
                    startLogReplicationRuntime(remoteCluster, session);
                } catch (Exception e) {
                    log.error("Failed to start log replication runtime for remote session {}, replication model {}, " +
                        "client {}", remoteCluster.getClusterId(), subscriber.getReplicationModel(),
                        subscriber.getClient());
                }
            }
        }
    }

    /**
     * Stop log replication for all the sink sites
     */
    public void stop() {
        sessionRuntimeMap.values().forEach(runtime -> {
            try {
                log.info("Stop log replication runtime to remote cluster id={}", runtime.getRemoteClusterId());
                runtime.stop();
            } catch (Exception e) {
                log.warn("Failed to stop log replication runtime to remote cluster id={}", runtime.getRemoteClusterId());
            }
        });
        sessionRuntimeMap.clear();
    }

    /**
     * Start Log Replication Runtime to a specific Sink Session
     */
    private void startLogReplicationRuntime(ClusterDescriptor remoteClusterDescriptor,
                                            LogReplicationSession session) {
        try {
            if (!sessionRuntimeMap.containsKey(session)) {
                log.info("Starting Log Replication Runtime for session {}", session);
                connect(remoteClusterDescriptor, session);
            } else {
                log.warn("Log Replication Runtime for session {}, already exists. Skip.",
                        session);
            }
        } catch (Exception e) {
            log.error("Caught exception, stop log replication runtime to {}", session, e);
            stopLogReplicationRuntime(session);
        }
    }

    /**
     * Connect to a remote Log Replicator, through a Log Replication Runtime.
     *
     * @throws InterruptedException
     */
    private void connect(ClusterDescriptor remoteCluster, LogReplicationSession session) throws InterruptedException {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    LogReplicationRuntimeParameters parameters = LogReplicationRuntimeParameters.builder()
                            .session(session)
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
                    CorfuLogReplicationRuntime replicationRuntime = new CorfuLogReplicationRuntime(parameters,
                        metadataManager, replicationConfigManager, session);
                    replicationRuntime.start();
                    sessionRuntimeMap.put(session, replicationRuntime);
                } catch (Exception e) {
                    log.error("Exception {}. Failed to connect to remote cluster for session {}. Retry after 1 second.",
                        e, session);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to connect to remote session.", e);
            throw e;
        }
    }

    /**
     * Stop Log Replication for a specific session
     */
    private void stopLogReplicationRuntime(LogReplicationSession session) {
        CorfuLogReplicationRuntime logReplicationRuntime = sessionRuntimeMap.get(session);
        if (logReplicationRuntime != null) {
            log.info("Stop log replication runtime for session {}", session);
            logReplicationRuntime.stop();
            sessionRuntimeMap.remove(session);
        } else {
            log.warn("Runtime not found for session {}", session);
        }
    }

    /**
     * Update Log Replication Runtime config id.
     */
    private void updateRuntimeConfigId(TopologyDescriptor newConfig) {
        sessionRuntimeMap.values().forEach(runtime -> runtime.updateFSMConfigId(newConfig));
    }

    /**
     * The notification of adding/removing Sinks with/without topology configId change.
     *
     * @param newTopology should have the same topologyConfigId as the current config
     * @param sinksToAdd the new sink clusters to be added
     * @param sinksToRemove sink clusters which are not found in the new topology
     * @param intersection Sink clusters found in both old and new topologies
     */
    // TODO: for multi-model support it should send sessions to add/remove instead of cluster IDs and this method will change
    public void processSinkChange(TopologyDescriptor newTopology, Set<String> sinksToAdd, Set<String> sinksToRemove,
                                  Set<String> intersection) {

        long oldTopologyConfigId = context.getTopology().getTopologyConfigId();
        context.setTopology(newTopology);

        Set<LogReplicationSession> sessions = newTopology.getSessions();

        // Remove Sinks that are not in the new config
        for(LogReplicationSession session : sessions) {
            if(sinksToRemove.contains(session.getSinkClusterId())) {
                stopLogReplicationRuntime(session);
                metadataManager.removeSession(session);
            }
        }

        // Start replication to newly added Sinks
        for(String clusterId : sinksToAdd) {
            ClusterDescriptor clusterInfo = newTopology.getSinkClusters().get(clusterId);

            for (ReplicationSubscriber subscriber : subscribers) {
                startLogReplicationRuntime(clusterInfo, new ReplicationSession(clusterId, subscriber));
            }
        }

        // The connection id or other transportation plugin's info could've changed for existing Sink clusters,
        // updating the routers will re-establish the connection to the correct endpoints/nodes
        for (String clusterId : intersection) {
            ClusterDescriptor clusterInfo = newTopology.getSinkClusters().get(clusterId);
            for (ReplicationSubscriber subscriber : subscribers) {
                sessionRuntimeMap.get(new ReplicationSession(clusterId, subscriber))
                    .updateRouterClusterDescriptor(clusterInfo);
            }
        }

        if (oldTopologyConfigId != newTopology.getTopologyConfigId()) {
            updateRuntimeConfigId(newTopology);
        }
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
            log.info("Enforce snapshot sync for remote cluster {}", runtime.getRemoteClusterId());
            runtime.getSourceManager().stopLogReplication();
            runtime.getSourceManager().startForcedSnapshotSync(event.getEventId());
        }
    }
}
