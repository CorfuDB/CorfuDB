package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class manages Log Replication for multiple remote (standby) clusters.
 */
@Slf4j
public class CorfuReplicationManager {

    // Keep map of remote cluster ID and the associated log replication runtime (an abstract
    // client to that cluster)
    private final Map<String, CorfuLogReplicationRuntime> runtimeToRemoteCluster = new HashMap<>();

    @Setter
    private LogReplicationContext context;

    private final NodeDescriptor localNodeDescriptor;

    private final CorfuRuntime corfuRuntime;

    private final Map<String, LogReplicationMetadataManager> metadataManagerMap;

    private final String pluginFilePath;

    private final LogReplicationConfigManager replicationConfigManager;

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
        this.localNodeDescriptor = localNodeDescriptor;
        this.replicationConfigManager = replicationConfigManager;
    }

    /**
     * Start Log Replication Manager, this will initiate a runtime against
     * each standby cluster, to further start log replication.
     */
    public void start() {
        for (ClusterDescriptor remoteCluster :
            context.getTopology().getStandbyClusters().values()) {
            try {
                startLogReplicationRuntime(remoteCluster);
            } catch (Exception e) {
                log.error("Failed to start log replication runtime for remote cluster {}", remoteCluster.getClusterId());
            }
        }
    }

    /**
     * Stop log replication for all the standby sites
     */
    public void stop() {
        runtimeToRemoteCluster.values().forEach(runtime -> {
            try {
                log.info("Stop log replication runtime to remote cluster id={}", runtime.getRemoteClusterId());
                runtime.stop();
            } catch (Exception e) {
                log.warn("Failed to stop log replication runtime to remote cluster id={}", runtime.getRemoteClusterId());
            }
        });
        runtimeToRemoteCluster.clear();
    }

    /**
     * Restart connection to remote cluster
     */
    public void restart(ClusterDescriptor remoteCluster) {
        stopLogReplicationRuntime(remoteCluster.getClusterId());
        startLogReplicationRuntime(remoteCluster);
    }

    /**
     * Start Log Replication Runtime to a specific standby Cluster
     */
    private void startLogReplicationRuntime(ClusterDescriptor remoteClusterDescriptor) {
        String remoteClusterId = remoteClusterDescriptor.getClusterId();
        try {
            if (!runtimeToRemoteCluster.containsKey(remoteClusterId)) {
                log.info("Starting Log Replication Runtime to Standby Cluster id={}", remoteClusterId);
                connect(remoteClusterDescriptor);
            } else {
                log.warn("Log Replication Runtime to remote cluster {}, already exists. Skipping init.", remoteClusterId);
            }
        } catch (Exception e) {
            log.error("Caught exception, stop log replication runtime to {}", remoteClusterDescriptor, e);
            stopLogReplicationRuntime(remoteClusterId);
        }
    }

    /**
     * Connect to a remote Log Replicator, through a Log Replication Runtime.
     *
     * @throws InterruptedException
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

    /**
     * Stop Log Replication to a specific standby Cluster
     */
    private void stopLogReplicationRuntime(String remoteClusterId) {
        CorfuLogReplicationRuntime logReplicationRuntime = runtimeToRemoteCluster.get(remoteClusterId);
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
     * Stop the current log replication event and start a full snapshot sync for the given remote cluster.
     */
    public void enforceSnapshotSync(DiscoveryServiceEvent event) {
        CorfuLogReplicationRuntime standbyRuntime = runtimeToRemoteCluster.get(event.getRemoteClusterInfo().getClusterId());
        if (standbyRuntime == null) {
            log.warn("Failed to start enforceSnapshotSync for cluster {} as it is not on the standby list.",
                    event.getRemoteClusterInfo());
        } else {
            log.info("EnforceSnapshotSync for cluster {}", standbyRuntime.getRemoteClusterId());
            standbyRuntime.getSourceManager().stopLogReplication();
            standbyRuntime.getSourceManager().startForcedSnapshotSync(event.getEventId());
        }
    }
}
