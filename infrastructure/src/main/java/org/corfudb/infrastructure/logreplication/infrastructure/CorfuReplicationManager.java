package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.runtime.CorfuRuntime;

import java.util.HashMap;
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

    // Keep map of remote cluster ID and the associated log replication runtime (an abstract
    // client to that cluster)
    private final Map<String, CorfuLogReplicationRuntime> runtimeToRemoteCluster = new HashMap<>();

    @Setter
    private LogReplicationContext context;

    private final NodeDescriptor localNodeDescriptor;

    @Getter
    private final CorfuRuntime corfuRuntime;

    private final Map<String, LogReplicationMetadataManager> metadataManagerMap;

    private TopologyDescriptor topology;

    private final LogReplicationContext replicationContext;

    private final Map<LogReplicationSession, LogReplicationRuntimeParameters> replicationSessionToRuntimeParams;

    /**
     * Constructor
     */
    public CorfuReplicationManager(LogReplicationContext context,
        NodeDescriptor localNodeDescriptor,
        Map<String, LogReplicationMetadataManager> metadataManagerMap,
        String pluginFilePath, CorfuRuntime corfuRuntime,
        LogReplicationConfigManager replicationConfigManager) {
        this.context = context;
        this.metadataManagerMap = metadataManagerMap;
        this.pluginFilePath = pluginFilePath;
        this.corfuRuntime = corfuRuntime;
        this.localNodeDescriptor = topology.getLocalNodeDescriptor();
        this.topology = topology;
        this.replicationContext = replicationContext;
        this.replicationSessionToRuntimeParams = new HashMap<>();
    }

    /**
     * Create Log Replication Runtime for a session, if not already created.
     */
    public void start() {
        for (ClusterDescriptor remoteCluster :
            context.getTopology().getStandbyClusters().values()) {
            try {
                startLogReplicationRuntime(remoteCluster);
            } catch (Exception e) {
                log.error("Failed to start log replication runtime for remote cluster {}", remoteCluster.getClusterId());
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
    private void connect(ClusterDescriptor remoteCluster) throws InterruptedException {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
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
                    CorfuLogReplicationRuntime replicationRuntime =
                        new CorfuLogReplicationRuntime(parameters,
                            metadataManagerMap.get(remoteCluster.clusterId),
                            replicationConfigManager);
                    replicationRuntime.start();
                    runtimeToRemoteCluster.put(remoteCluster.getClusterId(), replicationRuntime);
                } catch (Exception e) {
                    log.error("Exception {}. Failed to connect to remote cluster {}. Retry after 1 second.",
                            e, remoteCluster.getClusterId());
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to connect to remote cluster.", e);
            throw e;
        }
    }

    private void stopLogReplicationRuntime(LogReplicationSession session) {
        CorfuLogReplicationRuntime logReplicationRuntime = sessionRuntimeMap.get(session);
        if (logReplicationRuntime != null) {
            log.info("Stop log replication runtime to remote cluster id={}", remoteClusterId);
            logReplicationRuntime.stop();
            runtimeToRemoteCluster.remove(remoteClusterId);
        } else {
            log.warn("Runtime not found to remote cluster {}", remoteClusterId);
        }
    }

    /**
     * Update Log Replication Runtime config id.
     */
    private void updateRuntimeConfigId(TopologyDescriptor newConfig) {
        runtimeToRemoteCluster.values().forEach(runtime -> runtime.updateFSMConfigId(newConfig));
    }

    /**
     * The notification of change of adding/removing Sinks with/without topology configId change.
     *
     * @param newConfig should have the same topologyConfigId as the current config
     * @param sinksToAdd the new sink clusters to be added
     * @param sinksToRemove sink clusters which are not found in the new topology
     */
    public void processStandbyChange(TopologyDescriptor newConfig, Set<String> sinksToAdd, Set<String> sinksToRemove,
        Set<String> intersection) {

        long oldTopologyConfigId = context.getTopology().getTopologyConfigId();
        context.setTopology(newConfig);

        // Remove Sinks that are not in the new config
        for (String clusterId : sinksToRemove) {
            stopLogReplicationRuntime(clusterId);
        }

        // Start the newly added Sinks
        for (String clusterId : sinksToAdd) {
            ClusterDescriptor clusterInfo = newConfig.getStandbyClusters().get(clusterId);
            startLogReplicationRuntime(clusterInfo);
        }

        // The connection id or other transportation plugin's info could've changed for existing Sink cluster's,
        // updating the routers will re-establish the connection to the correct endpoints/nodes
        for (String clusterId : intersection) {
            ClusterDescriptor clusterInfo = newConfig.getStandbyClusters().get(clusterId);
            runtimeToRemoteCluster.get(clusterId).updateRouterClusterDescriptor(clusterInfo);
        }

        if (oldTopologyConfigId != newConfig.getTopologyConfigId()) {
            updateRuntimeConfigId(newConfig);
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
            log.info("Enforce snapshot sync for remote session {}", event.getSession());
            runtime.getSourceManager().stopLogReplication();
            runtime.getSourceManager().startForcedSnapshotSync(event.getEventId());
        }
    }
}
