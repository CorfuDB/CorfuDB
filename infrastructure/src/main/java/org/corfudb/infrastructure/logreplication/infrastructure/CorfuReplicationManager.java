package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * This class manages Log Replication for multiple remote (standby) clusters.
 */
@Slf4j
public class CorfuReplicationManager {

    public final static int PERCENTAGE_HUNDRED = 100;

    // Keep map of remote cluster ID and the associated log replication runtime (an abstract
    // client to that cluster)
    private Map<String, CorfuLogReplicationRuntime> runtimeToRemoteCluster = new HashMap<>();

    @Setter
    @Getter
    private TopologyDescriptor topology;

    private final LogReplicationContext context;

    private final NodeDescriptor localNodeDescriptor;

    private final CorfuRuntime corfuRuntime;

    private final LogReplicationMetadataManager metadataManager;

    private final String pluginFilePath;

    /**
     * Constructor
     */
    public CorfuReplicationManager(LogReplicationContext context, NodeDescriptor localNodeDescriptor,
                                   LogReplicationMetadataManager metadataManager, String pluginFilePath,
                                   CorfuRuntime corfuRuntime) {
        this.context = context;
        this.metadataManager = metadataManager;
        this.pluginFilePath = pluginFilePath;
        this.corfuRuntime = corfuRuntime;
        this.localNodeDescriptor = localNodeDescriptor;
    }

    /**
     * Start Log Replication Manager, this will initiate a runtime against
     * each standby cluster, to further start log replication.
     */
    public void start() {
        for (ClusterDescriptor remoteCluster : topology.getStandbyClusters().values()) {
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
                            .channelContext(context.getChannelContext())
                            .topologyConfigId(topology.getTopologyConfigId())
                            .keyStore(corfuRuntime.getParameters().getKeyStore())
                            .tlsEnabled(corfuRuntime.getParameters().isTlsEnabled())
                            .ksPasswordFile(corfuRuntime.getParameters().getKsPasswordFile())
                            .trustStore(corfuRuntime.getParameters().getTrustStore())
                            .tsPasswordFile(corfuRuntime.getParameters().getTsPasswordFile())
                            .build();
                    CorfuLogReplicationRuntime replicationRuntime = new CorfuLogReplicationRuntime(parameters, metadataManager);
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
     * The notification of change of adding/removing standby's without epoch change.
     *
     * @param newConfig has the same topologyConfigId as the current config
     */
    public void processStandbyChange(TopologyDescriptor newConfig) {
        if (newConfig.getTopologyConfigId() != topology.getTopologyConfigId()) {
            log.error("Detected changes in the topology. The new topology descriptor {} doesn't have the same " +
                    "topologyConfigId as the current one {}", newConfig, topology);
            return;
        }

        Map<String, ClusterDescriptor> newStandbys = newConfig.getStandbyClusters();
        Map<String, ClusterDescriptor> currentStandbys = topology.getStandbyClusters();
        newStandbys.keySet().retainAll(currentStandbys.keySet());
        Set<String> standbysToRemove = currentStandbys.keySet();
        standbysToRemove.removeAll(newStandbys.keySet());

        /*
         * Remove standbys that are not in the new config
         */
        for (String clusterId : standbysToRemove) {
            stopLogReplicationRuntime(clusterId);
            topology.removeStandbyCluster(clusterId);
        }

        //Start the standbys that are in the new config but not in the current config
        for (String clusterId : newConfig.getStandbyClusters().keySet()) {
            if (runtimeToRemoteCluster.get(clusterId) == null) {
                ClusterDescriptor clusterInfo = newConfig.getStandbyClusters().get(clusterId);
                topology.addStandbyCluster(clusterInfo);
                startLogReplicationRuntime(clusterInfo);
            }
        }
    }
}
