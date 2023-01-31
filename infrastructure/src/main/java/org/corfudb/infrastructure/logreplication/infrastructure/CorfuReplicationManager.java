package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
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

    // Keep map of remote session and the associated log replication runtime (an abstract client to that cluster)
    private final Map<ReplicationSession, CorfuLogReplicationRuntime> runtimeToRemoteSession = new HashMap<>();

    @Setter
    private LogReplicationContext replicationContext;

    private final NodeDescriptor localNodeDescriptor;

    private final CorfuRuntime corfuRuntime;

    private final Map<ReplicationSession, LogReplicationMetadataManager> metadataManagerMap;

    private final String pluginFilePath;

    private final LogReplicationUpgradeManager upgradeManager;

    /**
     * Constructor
     */
    public CorfuReplicationManager(LogReplicationContext replicationContext, NodeDescriptor localNodeDescriptor,
                                   Map<ReplicationSession, LogReplicationMetadataManager> metadataManagerMap,
                                   String pluginFilePath, CorfuRuntime corfuRuntime, LogReplicationUpgradeManager upgradeManager) {
        this.replicationContext = replicationContext;
        this.metadataManagerMap = metadataManagerMap;
        this.pluginFilePath = pluginFilePath;
        this.corfuRuntime = corfuRuntime;
        this.localNodeDescriptor = localNodeDescriptor;
        this.upgradeManager = upgradeManager;
    }

    /**
     * Start log replication runtime for a given remote cluster (sink), which further drives the log replication.
     */
    public void start(ClusterDescriptor remoteCluster, LogReplicationContext context) {
        for (ReplicationSubscriber subscriber : context.getConfig().getReplicationSubscriberToStreamsMap().keySet()) {
            try {
                startLogReplicationRuntime(remoteCluster, new ReplicationSession(remoteCluster.getClusterId(),
                    subscriber));
            } catch (Exception e) {
                log.error("Failed to start log replication runtime for remote session {}, replication model {}, " +
                    "client {}", remoteCluster.getClusterId(), subscriber.getReplicationModel(),
                    subscriber.getClient());
            }
        }
    }

    /**
     * Stop log replication for all the sink sites
     */
    public void stop() {
        runtimeToRemoteSession.values().forEach(runtime -> {
            try {
                log.info("Stop log replication runtime to remote cluster id={}", runtime.getRemoteClusterId());
                runtime.stop();
            } catch (Exception e) {
                log.warn("Failed to stop log replication runtime to remote cluster id={}", runtime.getRemoteClusterId());
            }
        });
        runtimeToRemoteSession.clear();
    }

    /**
     * Start Log Replication Runtime to a specific Sink Session
     */
    private void startLogReplicationRuntime(ClusterDescriptor remoteClusterDescriptor,
                                            ReplicationSession replicationSession) {
        try {
            if (!runtimeToRemoteSession.containsKey(replicationSession)) {
                log.info("Starting Log Replication Runtime for session {}", replicationSession);
                connect(remoteClusterDescriptor, replicationSession);
            } else {
                log.warn("Log Replication Runtime to remote session {}, already exists. Skipping init.",
                    replicationSession);
            }
        } catch (Exception e) {
            log.error("Caught exception, stop log replication runtime to {}", replicationSession, e);
            stopLogReplicationRuntime(replicationSession);
        }
    }

    /**
     * Connect to a remote Log Replicator, through a Log Replication Runtime.
     *
     * @throws InterruptedException
     */
    private void connect(ClusterDescriptor remoteCluster, ReplicationSession session) throws InterruptedException {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    LogReplicationRuntimeParameters parameters = LogReplicationRuntimeParameters.builder()
                            .localCorfuEndpoint(replicationContext.getLocalCorfuEndpoint())
                            .remoteClusterDescriptor(remoteCluster)
                            .localClusterId(localNodeDescriptor.getClusterId())
                            .replicationConfig(replicationContext.getConfig())
                            .pluginFilePath(pluginFilePath)
                            .topologyConfigId(replicationContext.getTopologyConfigId())
                            .keyStore(corfuRuntime.getParameters().getKeyStore())
                            .tlsEnabled(corfuRuntime.getParameters().isTlsEnabled())
                            .ksPasswordFile(corfuRuntime.getParameters().getKsPasswordFile())
                            .trustStore(corfuRuntime.getParameters().getTrustStore())
                            .tsPasswordFile(corfuRuntime.getParameters().getTsPasswordFile())
                            .maxWriteSize(corfuRuntime.getParameters().getMaxWriteSize())
                            .build();
                    CorfuLogReplicationRuntime replicationRuntime = new CorfuLogReplicationRuntime(parameters,
                        metadataManagerMap.get(session), upgradeManager, replicationContext, session);
                    replicationRuntime.start();
                    runtimeToRemoteSession.put(session, replicationRuntime);
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
     * Stop Log Replication to a specific Sink session
     */
    private void stopLogReplicationRuntime(ReplicationSession replicationSession) {
        CorfuLogReplicationRuntime logReplicationRuntime = runtimeToRemoteSession.get(replicationSession);
        if (logReplicationRuntime != null) {
            log.info("Stop log replication runtime for remote session {}", replicationSession);
            logReplicationRuntime.stop();
            runtimeToRemoteSession.remove(replicationSession);
        } else {
            log.warn("Runtime not found for remote session {}", replicationSession);
        }
    }

    /**
     * Update Log Replication Runtime config id.
     */
    private void updateRuntimeConfigId(TopologyDescriptor newConfig) {
        runtimeToRemoteSession.values().forEach(runtime -> runtime.updateFSMConfigId(newConfig));
    }

    /**
     * The notification of adding/removing Sinks with/without topology configId change.
     *
     * @param newConfig should have the same topologyConfigId as the current config
     * @param sinksToAdd the new sink clusters to be added
     * @param sinksToRemove sink clusters which are not found in the new topology
     * @param intersection Sink clusters found in both old and new topologies
     */
    public void processSinkChange(TopologyDescriptor newConfig, Set<String> sinksToAdd, Set<String> sinksToRemove,
                                  Set<String> intersection) {

        long oldTopologyConfigId = replicationContext.getTopologyConfigId();
        replicationContext.setTopologyConfigId(newConfig.getTopologyConfigId());

        // Get all subscribers
        Set<ReplicationSubscriber> subscribers = replicationContext.getConfig().getReplicationSubscriberToStreamsMap().keySet();

        // Remove Sinks that are not in the new config
        for (String clusterId : sinksToRemove) {
            for (ReplicationSubscriber subscriber : subscribers) {
                stopLogReplicationRuntime(new ReplicationSession(clusterId, subscriber));
            }
        }

        // Start the newly added Sinks
        for (String clusterId : sinksToAdd) {
            ClusterDescriptor clusterInfo = newConfig.getSinkClusters().get(clusterId);
            for (ReplicationSubscriber subscriber : subscribers) {
                startLogReplicationRuntime(clusterInfo, new ReplicationSession(clusterId, subscriber));
            }
        }

        // The connection id or other transportation plugin's info could've changed for existing Sink clusters,
        // updating the routers will re-establish the connection to the correct endpoints/nodes
        for (String clusterId : intersection) {
            ClusterDescriptor clusterInfo = newConfig.getSinkClusters().get(clusterId);
            for (ReplicationSubscriber subscriber : subscribers) {
                runtimeToRemoteSession.get(new ReplicationSession(clusterId, subscriber))
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
        CorfuLogReplicationRuntime sinkRuntime = runtimeToRemoteSession.get(
            ReplicationSession.getDefaultReplicationSessionForCluster(event.getRemoteClusterInfo().getClusterId()));
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
