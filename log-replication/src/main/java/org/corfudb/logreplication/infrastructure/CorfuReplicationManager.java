package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.cluster.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.cluster.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.cluster.ClusterDescriptor;
import org.corfudb.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.logreplication.send.LogReplicationEventMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;
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

    private final CorfuReplicationDiscoveryService discoveryService;

    private final String pluginFilePath;

    /**
     * Constructor
     *
     * @param topologyDescriptor description of active and standby cluster' of a given topology
     * @param logReplicationConfig log replication configuration
     * @param discoveryService
     */
    CorfuReplicationManager(TopologyDescriptor topologyDescriptor,
                            LogReplicationConfig logReplicationConfig,
                            NodeDescriptor localNodeDescriptor,
                            CorfuReplicationDiscoveryService discoveryService,
                            String pluginFilePath) {
        this.topologyDescriptor = topologyDescriptor;
        this.logReplicationConfig = logReplicationConfig;
        this.discoveryService = discoveryService;
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
            CorfuLogReplicationRuntime runtime = runtimeToRemoteCluster.get(remoteClusterId);

            // If we start from a stop state due to cluster switch over, we need to restart the consumer.
            runtime.getSourceManager().getLogReplicationFSM().startFSM(topologyDescriptor);

            LogReplicationEvent negotiationResult = startNegotiation(runtime);
            log.info("Log Replication Negotiation with {} result {}", remoteClusterId, negotiationResult);
            startLogReplication(runtime, negotiationResult);
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
                    CorfuLogReplicationRuntime replicationRuntime = new CorfuLogReplicationRuntime(parameters);
                    replicationRuntime.connect(discoveryService);
                    runtimeToRemoteCluster.put(remoteSiteInfo.getClusterId(), replicationRuntime);
                } catch (Exception e) {
                    log.error("Exception {}.  Failed to connect to remote cluster {}. Retry after 1 second.",
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
        if (newConfig.getSiteConfigID() != topologyDescriptor.getSiteConfigID()) {
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
     * Start replication process by put replication event to FSM.
     *
     * @param runtime
     * @param negotiationResult
     */
    private void startLogReplication(CorfuLogReplicationRuntime runtime, LogReplicationEvent negotiationResult) {
        switch (negotiationResult.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                log.info("Start Snapshot Sync Replication");
                runtime.startSnapshotSync();
                break;
            case SNAPSHOT_WAIT_COMPLETE:
                log.info("Should Start Snapshot Sync Phase II,but for now just restart full snapshot sync");
                /*
                 * TODO: xiaoqin: Right now it is hard to put logic for SNAPSHOT_WAIT_COMPLETE
                 * replace it with SNAPSHOT_SYNC_REQUEST, and will re-examine it later.
                 */
                runtime.startSnapshotSync();
                break;
            case REPLICATION_START:
                log.info("Start Log Entry Sync Replication");
                runtime.startLogEntrySync(negotiationResult);
                break;
            default:
                log.info("Invalid Negotiation result. Re-trigger discovery.");
                break;
        }
    }

    /**
     * Start Log Replication Negotiation.
     *
     * In this stage the active cluster negotiates with the standby cluster to determine
     * the log replication start point. This can be to continue log entry sync (delta) from an earlier point
     * (as data is still available) or to start from a snapshot sync (full).
     *
     * @throws LogReplicationNegotiationException
     */
    private LogReplicationEvent startNegotiation(CorfuLogReplicationRuntime logReplicationRuntime)
            throws LogReplicationNegotiationException {
        try {
            // TODO: We might want to enclose this in an IRetry
            LogReplicationNegotiationResponse negotiationResponse = logReplicationRuntime.startNegotiation();
            log.trace("Negotiation Response received: {} ", negotiationResponse);

            // Determine if we should proceed with Snapshot Sync or Log Entry Sync
            return processNegotiationResponse(negotiationResponse);

        } catch (Exception e) {
            log.error("Caught exception during log replication negotiation to {} ", logReplicationRuntime.getParameters().getRemoteClusterDescriptor(), e);
            throw new LogReplicationNegotiationException(e.getCause().getMessage());
        }
    }

    /**
     * It will decide to do a full snapshot sync or log entry sync according to the metadata received from the standby site.
     *
     * @param negotiationResponse
     * @return
     * @throws LogReplicationNegotiationException
     */
    private LogReplicationEvent processNegotiationResponse(LogReplicationNegotiationResponse negotiationResponse)
            throws LogReplicationNegotiationException {
        /*
         * If the version are different, report an error.
         */
        if (!negotiationResponse.getVersion().equals(discoveryService.getLogReplicationMetadataManager().getVersion())) {
            log.error("The active site version {} is different from standby site version {}",
                    discoveryService.getLogReplicationMetadataManager().getVersion(), negotiationResponse.getVersion());
            throw new LogReplicationNegotiationException(" Mismatch of version number");
        }

        /*
         * The standby site has a smaller config ID, redo the discovery for this standby site when
         * getting a new notification of the site config change if this standby is in the new config.
         */
        if (negotiationResponse.getSiteConfigID() < negotiationResponse.getSiteConfigID()) {
            log.error("The active site configID {} is bigger than the standby configID {} ",
                    discoveryService.getLogReplicationMetadataManager().getSiteConfigID(), negotiationResponse.getSiteConfigID());
            throw new LogReplicationNegotiationException("Mismatch of configID");
        }

        /*
         * The standby site has larger config ID, redo the whole discovery for the active site
         * it will be triggered by a notification of the site config change.
         */
        if (negotiationResponse.getSiteConfigID() > negotiationResponse.getSiteConfigID()) {
            log.error("The active site configID {} is smaller than the standby configID {} ",
                    discoveryService.getLogReplicationMetadataManager().getSiteConfigID(), negotiationResponse.getSiteConfigID());
            throw new LogReplicationNegotiationException("Mismatch of configID");
        }

        /*
         * Now the active and standby have the same version and same configID.
         */

        /*
         * Get the current log head.
         */
        long logHead = corfuRuntime.getAddressSpaceView().getTrimMark().getSequence();
        LogReplicationEvent event;

        /*
         * It is a fresh start, start snapshot full sync.
         * Following is an example that metadata value indicates a fresh start, no replicated data at standby site:
         * "topologyConfigId": "10"
         * "version": "release-1.0"
         * "snapshotStart": "-1"
         * "snapshotSeqNum": " -1"
         * "snashotTransferred": "-1"
         * "snapshotApplied": "-1"
         * "lastLogEntryProcessed": "-1"
         */
        if (negotiationResponse.getSnapshotStart() == -1) {
            negotiationResponse.getLastLogProcessed();
            event = new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
            return event;
        }

        /*
         * If it is in the snapshot full sync phase I, transferring data, restart the snapshot full sync.
         * An example of in Snapshot Sync Phase I, transfer phase:
         * "topologyConfigId": "10"
         * "version": "release-1.0"
         * "snapshotStart": "100"
         * "snapshotSeqNum": " 88"
         * "snashotTransferred": "-1"
         * "snapshotApplied": "-1"
         * "lastLogEntryProcessed": "-1"
         */
        if (negotiationResponse.getSnapshotStart() > negotiationResponse.getSnapshotTransferred()) {
            event =  new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
            log.info("Get the negotiation response {} and will start replication event {}.",
                    negotiationResponse, event);
            return event;
        }

        /*
         * If it is in the snapshot full sync phase II:
         * the data has been transferred to the standby site and the the standby site is applying data from shadow streams
         * to the real streams.
         * It doesn't need to transfer the data again, just send a SNAPSHOT_COMPLETE message to the standby site.
         * An example of in Snapshot sync phase II: applying phase
         * "topologyConfigId": "10"
         * "version": "release-1.0"
         * "snapshotStart": "100"
         * "snapshotSeqNum": " 88"
         * "snashotTransferred": "100"
         * "snapshotApplied": "-1"
         * "lastLogEntryProcessed": "-1"
         */
        if (negotiationResponse.getSnapshotStart() == negotiationResponse.getSnapshotTransferred() &&
                negotiationResponse.getSnapshotTransferred() > negotiationResponse.getSnapshotApplied()) {
            event =  new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_WAIT_COMPLETE,
                    new LogReplicationEventMetadata(LogReplicationEventMetadata.getNIL_UUID(), negotiationResponse.getSnapshotStart()));
            log.info("Get the negotiation response {} and will start replication event {}.",
                    negotiationResponse, event);
            return event;
        }

        /* If it is in log entry sync state, continues log entry sync state.
         * An example to show the standby site is in log entry sync phase.
         * A full snapshot transfer based on timestamp 100 has been completed, and this standby has processed all log entries
         * between 100 to 200. A log entry sync should be restart if log entry 201 is not trimmed.
         * Otherwise, start a full snapshpt full sync.
         * "topologyConfigId": "10"
         * "version": "release-1.0"
         * "snapshotStart": "100"
         * "snapshotSeqNum": " 88"
         * "snashotTransferred": "100"
         * "snapshotApplied": "100"
         * "lastLogEntryProcessed": "200"
         */
        if (negotiationResponse.getSnapshotStart() == negotiationResponse.getSnapshotTransferred() &&
                negotiationResponse.getSnapshotStart() == negotiationResponse.getSnapshotApplied() &&
                negotiationResponse.getLastLogProcessed() >= negotiationResponse.getSnapshotStart()) {
            /*
             * If the next log entry is not trimmed, restart with log entry sync,
             * otherwise, start snapshot full sync.
             */
            if (logHead <= negotiationResponse.getLastLogProcessed() + 1) {
                event = new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_START,
                        new LogReplicationEventMetadata(LogReplicationEventMetadata.getNIL_UUID(), negotiationResponse.getLastLogProcessed()));
                } else {
                event = new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
            }

            log.info("Get the negotiation response {} and will start replication event {}.",
                    negotiationResponse, event);

            return event;
        }

        /*
         * For other scenarios, the standby site is in a notn-recognizable state, trigger a snapshot full sync.
         */
        log.error("Could not recognize the standby site state according to the response {}, will restart with a snapshot full sync event " ,
                negotiationResponse);
        return new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
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
