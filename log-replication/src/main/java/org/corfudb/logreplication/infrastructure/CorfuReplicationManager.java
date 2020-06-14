package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.LogReplicationTransportType;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
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

@Slf4j
public class CorfuReplicationManager {

    public final static int PERCENTAGE_BASE = 100;
    
    /*
     * The map of remote site endpoints and the associated log replication runtime (client)
     */
    Map<String, CorfuLogReplicationRuntime> remoteSiteRuntimeMap = new HashMap<>();

    enum LogReplicationNegotiationResultType {
        SNAPSHOT_SYNC,
        LOG_ENTRY_SYNC,
        LEADERSHIP_LOST,
        CONNECTION_LOST,
        ERROR,  //Due to wrong version number etc
        UNKNOWN
    }

    LogReplicationTransportType transport;

    @Setter
    @Getter
    CrossSiteConfiguration crossSiteConfig;

    CorfuInterClusterReplicationServerNode replicationServerNode;

    CorfuReplicationDiscoveryService discoveryService;

    /*
     * Get the current max stream tail while preparing a role type change
     */
    long prepareSiteRoleChangeStreamTail;

    long totalNumEntriesToSend;

    CorfuReplicationManager(LogReplicationTransportType transport, CrossSiteConfiguration crossSiteConfig,
        CorfuInterClusterReplicationServerNode replicationServerNode, CorfuReplicationDiscoveryService discoveryService) {
        prepareSiteRoleChangeStreamTail = Address.NON_ADDRESS;
        totalNumEntriesToSend = 0;
        this.transport = transport;
        this.crossSiteConfig = crossSiteConfig;
        this.replicationServerNode = replicationServerNode;
        this.discoveryService = discoveryService;
    }

    /**
     * Connect log replication to a remote site.
     *
     * @throws InterruptedException
     */
    public void connect(LogReplicationNodeInfo localNode, CrossSiteConfiguration.SiteInfo siteInfo,
        CorfuReplicationDiscoveryService discoveryService) throws InterruptedException {

        log.trace("Setup runtime's from local node to remote site {}", siteInfo.getSiteId());

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    /*
                     * TODO (Xiaoqin Ma): shouldn't it connect only to the lead node on the remote site?
                     * It needs a runtime to do the negotiation with non leader remote too.
                     */
                    for (LogReplicationNodeInfo nodeInfo : siteInfo.getNodesInfo()) {
                        LogReplicationRuntimeParameters parameters = LogReplicationRuntimeParameters.builder()
                                .localCorfuEndpoint(localNode.getCorfuEndpoint())
                                .remoteLogReplicationServerEndpoint(nodeInfo.getEndpoint())
                                .remoteSiteId(nodeInfo.getSiteId())
                                .transport(transport)
                                .localSiteId(localNode.getSiteId())
                                .replicationConfig(replicationServerNode.getLogReplicationConfig())
                                .build();
                        CorfuLogReplicationRuntime replicationRuntime = new CorfuLogReplicationRuntime(parameters);
                        replicationRuntime.connect(discoveryService);
                        nodeInfo.setRuntime(replicationRuntime);
                    }
                    LogReplicationNodeInfo leader = siteInfo.getRemoteLeader();
                    log.info("connect to site {} lead node {}:{}", siteInfo.getSiteId(), leader.getIpAddress(), leader.getPortNum());
                    remoteSiteRuntimeMap.put(siteInfo.getSiteId(), leader.getRuntime());
                } catch (Exception e) {
                    log.error("Exception {}.  Failed to connect to remote site {}. Retry after 1 second.",
                        e, siteInfo.getSiteId());
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to connect to remote site.", e);
            throw e;
        }
    }

    /**
     * Once determined this is a Lead Sender (on primary site), connect and setup log replication.
     */
    private void startLogReplication(LogReplicationNodeInfo localNode, String siteId) {
        CrossSiteConfiguration.SiteInfo remoteSite = crossSiteConfig.getStandbySites().get(siteId);
        log.info("Start Log Replication to Standby Site {}", siteId);

        try {
            /*
             * a clean setup of the replication has done for this remote site
             */
            if (remoteSiteRuntimeMap.get(siteId) != null) {
                return;
            }

            connect(localNode, remoteSite, discoveryService);

            CorfuLogReplicationRuntime runtime = remoteSiteRuntimeMap.get(siteId);

            /*
             * If we start from a stop state due to site switch over, we need to restart the consumer.
             */
            runtime.getSourceManager().getLogReplicationFSM().startFSM(crossSiteConfig);

            LogReplicationEvent negotiationResult = startNegotiation(runtime);
            log.info("Log Replication Negotiation with {} result {}", siteId, negotiationResult);
            replicate(runtime, negotiationResult);
        } catch (Exception e) {
            log.error("Will stop this remote site replicaiton as caught an exception", e);
            /*
             * The remote runtime will be stopped and removed from the runtimeMap.
             */
            stopLogReplication(siteId);
        }
    }


    /**
     * Stop the current runtime and reestablish runtime and query the new leader.
     * @param localNode
     * @param siteId
     */
    public void restartLogReplication(LogReplicationNodeInfo localNode, String siteId) {
        stopLogReplication(siteId);
        startLogReplication(localNode, siteId);
    }

    public void startLogReplication(LogReplicationNodeInfo nodeInfo) {
        for (CrossSiteConfiguration.SiteInfo remoteSite : crossSiteConfig.getStandbySites().values()) {
            try {
                startLogReplication(nodeInfo, remoteSite.getSiteId());
            } catch (Exception e) {
                log.error("Failed to start log replication to remote site {}", remoteSite.getSiteId());
                /*
                 * remove from the standby list as the discovery site will get notification of the change when this site become
                 * stable again.
                 */
                crossSiteConfig.getStandbySites().remove(remoteSite.getSiteId());
            }
        }
    }

    /**
     * The notification of change of adding/removing standbys without epoch change.
     * @param newConfig has the same siteConfigId as the current config
     */
    public void processStandbyChange(LogReplicationNodeInfo nodeInfo, CrossSiteConfiguration newConfig) {
        if (newConfig.getSiteConfigID() != crossSiteConfig.getSiteConfigID()) {
            log.error("the new config {} doesn't have the same siteConfigId as the current one {}", newConfig, crossSiteConfig);
            return;
        }

        Map<String, CrossSiteConfiguration.SiteInfo> newStandbys = newConfig.getStandbySites();
        Map<String, CrossSiteConfiguration.SiteInfo> currentStandbys = crossSiteConfig.getStandbySites();
        newStandbys.keySet().retainAll(currentStandbys.keySet());
        Set<String> standbysToRemove = currentStandbys.keySet();
        standbysToRemove.removeAll(newStandbys.keySet());

        /*
         * Remove standbys that are not in the new config
         */
        for (String siteID : standbysToRemove) {
            stopLogReplication(siteID);
            crossSiteConfig.removeStandbySite(siteID);
        }

        /*
         * Start the standbys that are in the new config but not in the current config
         */
        for (String siteID : newConfig.getStandbySites().keySet()) {
            if (remoteSiteRuntimeMap.get(siteID) == null) {
                CrossSiteConfiguration.SiteInfo siteInfo = newConfig.getStandbySites().get(siteID);
                crossSiteConfig.addStandbySite(siteInfo);
                startLogReplication(nodeInfo, siteInfo.getSiteId());
            }
        }
    }

    /**
     * Start replication process by put replication event to FSM.
     * @param runtime
     * @param negotiationResult
     */
    private void replicate(CorfuLogReplicationRuntime runtime, LogReplicationEvent negotiationResult) {
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
     * Stop the log replication for the given remote site.
     * @param remoteSiteId
     */
    public void stopLogReplication(String remoteSiteId) {
        CrossSiteConfiguration.SiteInfo siteInfo = crossSiteConfig.getStandbySites().get(remoteSiteId);
        for (LogReplicationNodeInfo nodeInfo : siteInfo.getNodesInfo()) {
            CorfuLogReplicationRuntime runtime = nodeInfo.getRuntime();
            if (runtime != null) {
                nodeInfo.stopRuntime();
            }
        }
        remoteSiteRuntimeMap.remove(remoteSiteId);
    }

    /**
     * Stop log replication for all the standby sites.
     */
    public void stopLogReplication() {
        for(String siteId: crossSiteConfig.getStandbySites().keySet()) {
            stopLogReplication(siteId);
        }
    }

    /**
     * Start negotiation with a standby site.
     * @param logReplicationRuntime
     * @return
     * @throws LogReplicationNegotiationException
     */
    private LogReplicationEvent startNegotiation(CorfuLogReplicationRuntime logReplicationRuntime)
            throws LogReplicationNegotiationException {

        LogReplicationNegotiationResponse negotiationResponse;

        try {
            // TODO(Anny) : IRetry...
            negotiationResponse = logReplicationRuntime.startNegotiation();
            log.trace("Negotiation Response received: {} ", negotiationResponse);
        } catch (Exception e) {
            log.error("Caught exception during log replication negotiation to {} ", logReplicationRuntime.getParameters().getRemoteLogReplicationServerEndpoint(), e);
            throw new LogReplicationNegotiationException(e.getCause().getMessage());
        }

        /*
         * Determine if we should proceed with Snapshot Sync or Log Entry Sync
         */
        return processNegotiationResponse(negotiationResponse);
    }

    /**
     * It will decide to do a full snapshot sync or log entry sync according to the metadata received from the standby site.
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
         * it will be triggerred by a notification of the site config change.
         */
        if (negotiationResponse.getSiteConfigID() > negotiationResponse.getSiteConfigID()) {
            log.error("The active site configID {} is smaller than the standby configID {} ",
                    discoveryService.getLogReplicationMetadataManager().getSiteConfigID(), negotiationResponse.getSiteConfigID());
            throw new LogReplicationNegotiationException("Mismatch of configID");
        }

        /*
         * Now the active and standby have the same version and same configID.
         */

        CrossSiteConfiguration.SiteInfo siteInfo = crossSiteConfig.getStandbySites().values().iterator().next();
        CorfuRuntime runtime = siteInfo.getNodesInfo().get(0).getRuntime().getCorfuRuntime();

        /*
         * Get the current log head.
         */
        long logHead = runtime.getAddressSpaceView().getTrimMark().getSequence();
        LogReplicationEvent event;

        /*
         * It is a fresh start, start snapshot full sync.
         * Following is an example that metadata value indicates a fresh start, no replicated data at standby site:
         * "siteConfigID": "10"
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
         * "siteConfigID": "10"
         * "version": "release-1.0"
         * "snapshotStart": "100"
         * "snapshotSeqNum": " 88"
         * "snashotTransferred": "-1"
         * "snapshotApplied": "-1"
         * "lastLogEntryProcessed": "-1"
         */
        if (negotiationResponse.getSnapshotStart() > negotiationResponse.getSnapshotTransferred()) {
            event =  new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
            log.info("Get the negotionation response {} and will start replication event {}.",
                    negotiationResponse, event);
            return event;
        }

        /*
         * If it is in the snapshot full sync phase II:
         * the data has been transferred to the standby site and the the standby site is applying data from shadow streams
         * to the real streams.
         * It doesn't need to transfer the data again, just send a SNAPSHOT_COMPLETE message to the standby site.
         * An example of in Snapshot sync phase II: applying phase
         * "siteConfigID": "10"
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
            log.info("Get the negotionation response {} and will start replication event {}.",
                    negotiationResponse, event);
            return event;
        }

        /* If it is in log entry sync state, continues log entry sync state.
         * An example to show the standby site is in log entry sync phase.
         * A full snapshot transfer based on timestamp 100 has been completed, and this standby has processed all log entries
         * between 100 to 200. A log entry sync should be restart if log entry 201 is not trimmed.
         * Otherwise, start a full snapshpt full sync.
         * "siteConfigID": "10"
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

            log.info("Get the negotionation response {} and will start replication event {}.",
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
        CrossSiteConfiguration.SiteInfo siteInfo = crossSiteConfig.getStandbySites().values().iterator().next();
        LogReplicationNodeInfo nodeInfo = siteInfo.getNodesInfo().get(0);
        return nodeInfo.getRuntime().getMaxStreamTail();
    }

    /**
     * Given a timestamp, calculate how many entries to be sent for all replicated streams.
     * @param timestamp
     * @return
     */
    long queryEntriesToSent(long timestamp) {
        int totalNumEnries = 0;

        for (CorfuLogReplicationRuntime runtime: remoteSiteRuntimeMap.values()) {
            totalNumEnries += runtime.getNumEntriesToSend(timestamp);
        }

        return totalNumEnries;
    }

    /**
     * Query the current all replication stream log tail and remember the max stream tail.
     * Query each standby site information according to the ack information to calculate the number of
     * msgs to be sent out.
     */
    public void prepareSiteRoleChange() {
        prepareSiteRoleChangeStreamTail = queryStreamTail();
        totalNumEntriesToSend = queryEntriesToSent(prepareSiteRoleChangeStreamTail);
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

        long currentNumEntriesToSend = queryEntriesToSent(prepareSiteRoleChangeStreamTail);
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

    /**
     * Stop all replication.
     */
    public void shutdown() {
        stopLogReplication();
    }
}
