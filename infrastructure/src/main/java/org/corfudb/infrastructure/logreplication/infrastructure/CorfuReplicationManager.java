package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
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
 * This class manages Log Replication for multiple remote (standby) cluster's.
 */
@Slf4j
public class CorfuReplicationManager {

    public final static int PERCENTAGE_BASE = 100;

    // Keep map of remote cluster ID and the associated log replication runtime (an abstract
    // client to that cluster)
    private Map<String, CorfuLogReplicationRuntime> runtimeToRemoteCluster = new HashMap<>();

    @Setter
    @Getter
    private volatile TopologyDescriptor topologyDescriptor;

    private final LogReplicationConfig logReplicationConfig;

    private final NodeDescriptor localNodeDescriptor;

    private final CorfuRuntime corfuRuntime;

    // TODO (Xiaoqin Ma): can you please add a description on this variable's meaning
    private long prepareSiteRoleChangeStreamTail;

    private long totalNumEntriesToSend;

    private final LogReplicationMetadataManager metadataManager;

    private final String pluginFilePath;

    /**
     * Constructor
     *
     * @param topologyDescriptor description of active and standby cluster' of a given topology
     * @param logReplicationConfig log replication configuration
     */
    public CorfuReplicationManager(TopologyDescriptor topologyDescriptor,
                            LogReplicationConfig logReplicationConfig,
                            NodeDescriptor localNodeDescriptor,
                            LogReplicationMetadataManager metadataManager,
                            String pluginFilePath) {
        this.topologyDescriptor = topologyDescriptor;
        this.logReplicationConfig = logReplicationConfig;
        this.metadataManager = metadataManager;
        this.pluginFilePath = pluginFilePath;

        this.localNodeDescriptor = localNodeDescriptor;
        this.prepareSiteRoleChangeStreamTail = Address.NON_ADDRESS;
        this.totalNumEntriesToSend = 0;

        corfuRuntime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(topologyDescriptor.getActiveCluster().getNodesDescriptors().get(0).getCorfuEndpoint());
        corfuRuntime.connect();
    }

    /**
     * Start log replication to every standby cluster
     */
    public void start() {
        for (ClusterDescriptor remoteCluster : topologyDescriptor.getStandbyClusters().values()) {
            try {
                startLogReplication(remoteCluster);
            } catch (Exception e) {
                log.error("Failed to start log replication to remote cluster {}", remoteCluster.getClusterId());

                // Remove cluster from the list of standby's, as the cluster discovery process will receive
                // change notification when this site becomes stable/available again.
                topologyDescriptor.getStandbyClusters().remove(remoteCluster.getClusterId());
            }
        }
    }

    /**
     * Stop log replication for all the standby sites
     */
    public void stop() {
        for(String siteId: topologyDescriptor.getStandbyClusters().keySet()) {
            stopLogReplication(siteId);
        }
    }

    /**
     * Stop the current runtime, reestablish runtime and query the new leader.
     */
    public void restart(ClusterDescriptor remoteSiteInfo) {
        stopLogReplication(remoteSiteInfo.getClusterId());
        startLogReplication(remoteSiteInfo);
    }

    /**
     * Start Log Replication to a specific standby Cluster
     */
    private void startLogReplication(ClusterDescriptor remoteClusterDescriptor) {

        String remoteClusterId = remoteClusterDescriptor.getClusterId();

        try {
            log.info("Start Log Replication to Standby Cluster id={}", remoteClusterId);

            // a clean start up of the replication has done for this remote cluster
            if (runtimeToRemoteCluster.get(remoteClusterId) != null) {
                return;
            }

            connect(localNodeDescriptor, remoteClusterDescriptor);
        } catch (Exception e) {
            log.error("Caught exception, stop this cluster replication", e);
            // The remote runtime will be stopped and removed from the runtimeMap.
            stopLogReplication(remoteClusterId);
        }
    }

    /**
     * Connect Local Log Replicator to a remote Log Replicator.
     *
     * @throws InterruptedException
     */
    private void connect(NodeDescriptor localNode, ClusterDescriptor remoteSiteInfo) throws InterruptedException {

        log.trace("Setup log replication runtime from local node {} to remote cluster {}", localNode.getEndpoint(),
                remoteSiteInfo.getClusterId());

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    LogReplicationRuntimeParameters parameters = LogReplicationRuntimeParameters.builder()
                            .localCorfuEndpoint(localNode.getCorfuEndpoint())
                            .remoteClusterDescriptor(remoteSiteInfo)
                            .localClusterId(localNode.getClusterId())
                            .replicationConfig(logReplicationConfig)
                            .pluginFilePath(pluginFilePath)
                            .build();
                    CorfuLogReplicationRuntime replicationRuntime = new CorfuLogReplicationRuntime(parameters, metadataManager,
                            topologyDescriptor.getTopologyConfigId());
                    replicationRuntime.connect();
                    runtimeToRemoteCluster.put(remoteSiteInfo.getClusterId(), replicationRuntime);
                } catch (Exception e) {
                    log.error("Exception {}. Failed to connect to remote cluster {}. Retry after 1 second.",
                            e, remoteSiteInfo.getClusterId());
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
    private void stopLogReplication(String remoteClusterId) {
        CorfuLogReplicationRuntime runtime = runtimeToRemoteCluster.get(remoteClusterId);
        if (runtime != null) {
            runtime.stop();
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
        if (newConfig.getTopologyConfigId() != topologyDescriptor.getTopologyConfigId()) {
            log.error("Detected changes in the topology. The new topology descriptor {} doesn't have the same " +
                    "siteConfigId as the current one {}", newConfig, topologyDescriptor);
            return;
        }

        Map<String, ClusterDescriptor> newStandbys = newConfig.getStandbyClusters();
        Map<String, ClusterDescriptor> currentStandbys = topologyDescriptor.getStandbyClusters();
        newStandbys.keySet().retainAll(currentStandbys.keySet());
        Set<String> standbysToRemove = currentStandbys.keySet();
        standbysToRemove.removeAll(newStandbys.keySet());

        /*
         * Remove standbys that are not in the new config
         */
        for (String siteID : standbysToRemove) {
            stopLogReplication(siteID);
            topologyDescriptor.removeStandbySite(siteID);
        }

        //Start the standbys that are in the new config but not in the current config
        for (String clusterId : newConfig.getStandbyClusters().keySet()) {
            if (runtimeToRemoteCluster.get(clusterId) == null) {
                ClusterDescriptor clusterInfo = newConfig.getStandbyClusters().get(clusterId);
                topologyDescriptor.addStandbySite(clusterInfo);
                startLogReplication(clusterInfo);
            }
        }
    }



    /**
     *
     * @return max tail of all relevant streams.
     */
    long queryStreamTail() {
        Set<String> streamsToReplicate = logReplicationConfig.getStreamsToReplicate();
        long maxTail = Address.NON_ADDRESS;
        for (String s : streamsToReplicate) {
            UUID currentUUID = CorfuRuntime.getStreamID(s);
            Map<UUID, Long> tailMap = corfuRuntime.getAddressSpaceView().getAllTails().getStreamTails();
            Long currentTail = tailMap.get(currentUUID);
            if (currentTail != null) {
                maxTail = Math.max(maxTail, currentTail);
            }
        }
        return maxTail;
    }

    /**
     * Given a timestamp, calculate how many entries to be sent for all replicated streams.
     *
     * @param timestamp
     */
    long queryEntriesToSend(long timestamp) {
        int totalNumEntries = 0;

        for (CorfuLogReplicationRuntime runtime: runtimeToRemoteCluster.values()) {
            totalNumEntries += runtime.getNumEntriesToSend(timestamp);
        }

        return totalNumEntries;
    }

    /**
     * Query the current all replication stream log tail and remember the max stream tail.
     * Query each standby site information according to the ack information to calculate the number of
     * msgs to be sent out.
     */
    public void prepareSiteRoleChange() {
        prepareSiteRoleChangeStreamTail = queryStreamTail();
        totalNumEntriesToSend = queryEntriesToSend(prepareSiteRoleChangeStreamTail);
    }

    /**
     * Query the all replication stream log tail and calculate the number of messages to be sent.
     * If the max tail has changed, give 0 percent has done.
     * @return Percentage of work has been done, when it return 100, the replication is done.
     */
    public int queryReplicationStatus() {
        long maxTail = queryStreamTail();

        /*
         * If the tail has been moved, reset the base calculation
         */
        if (maxTail > prepareSiteRoleChangeStreamTail) {
            prepareSiteRoleChange();
        }

        long currentNumEntriesToSend = queryEntriesToSend(prepareSiteRoleChangeStreamTail);
        log.debug("maxTail {} totalNumEntriesToSend  {}  currentNumEntriesToSend {}", maxTail, totalNumEntriesToSend, currentNumEntriesToSend);

        if (totalNumEntriesToSend == 0 || currentNumEntriesToSend == 0)
            return PERCENTAGE_BASE;

        /*
         * percentage of has been sent
         * as the currentNumEntriesToSend is not zero, the percent should not be 100%
         */
        int percent = (int)((totalNumEntriesToSend - currentNumEntriesToSend)*PERCENTAGE_BASE/totalNumEntriesToSend);
        if (percent == PERCENTAGE_BASE) {
            percent = PERCENTAGE_BASE - 1;
        }

        return percent;
    }
}
