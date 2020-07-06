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
    private TopologyDescriptor topology;

    private final LogReplicationContext context;

    private final NodeDescriptor localNodeDescriptor;

    private final CorfuRuntime corfuRuntime;

    // TODO (Xiaoqin Ma): can you please add a description on this variable's meaning
    private long prepareClusterRoleChangeLogTail;

    private long totalNumEntriesToSend;

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
        this.prepareClusterRoleChangeLogTail = Address.NON_ADDRESS;
        this.totalNumEntriesToSend = 0;
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

    /**
     * Query max stream tail for all streams to be replicated.
     *
     * @return max tail of all relevant streams.
     */
    private long queryStreamTail() {
        Set<String> streamsToReplicate = context.getConfig().getStreamsToReplicate();
        long maxTail = Address.NON_ADDRESS;
        Map<UUID, Long> tailMap = corfuRuntime.getAddressSpaceView().getAllTails().getStreamTails();
        for (String s : streamsToReplicate) {
            UUID currentUUID = CorfuRuntime.getStreamID(s);
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
    private long queryEntriesToSend(long timestamp) {
        // TODO(Xiaoqin Ma / Nan) : is not exactly how many entries we need to send?
        //  Because we only transfer some streams, and getNumEntriesToSend returns (maxStreamTail - ackedTimeStamp),
        //  which will count other log entries. Besides, for loop will amplify totalNumEntries multiple times.

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
    public void prepareClusterRoleChange() {
        prepareClusterRoleChangeLogTail = queryStreamTail();
        totalNumEntriesToSend = queryEntriesToSend(prepareClusterRoleChangeLogTail);
    }

    /**
     * Query the all replication stream log tail and calculate the number of messages to be sent.
     * If the max tail has changed, give 0 percent has done.
     *
     * @return Percentage of work has been done, when it return 100, the replication is done.
     */
    public int queryReplicationStatus() {
        long maxTail = queryStreamTail();

        /*
         * If the tail has moved, reset the base calculation
         */
        if (maxTail > prepareClusterRoleChangeLogTail) {
            prepareClusterRoleChange();
        }

        // TODO(Xiaoqin Ma/Nan): if the max stream tail moves, it calls prepareSiteRoleChange(), which
        //  call queryStreamTail() one more time, and will update totalNumEntriesToSend.
        //  Then it call queryEntriesToSend() again, will get a pretty close result as currentNumEntriesToSend.
        //  So percent calculation will always return a 0.
        long currentNumEntriesToSend = queryEntriesToSend(prepareClusterRoleChangeLogTail);
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
