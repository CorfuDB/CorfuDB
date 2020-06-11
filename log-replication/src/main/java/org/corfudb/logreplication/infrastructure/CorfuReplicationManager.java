package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

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
    // Keep map of remote site endpoints and the associated log replication runtime (client)
    Map<String, CorfuLogReplicationRuntime> remoteSiteRuntimeMap = new HashMap<>();

    enum LogReplicationNegotiationResultType {
        SNAPSHOT_SYNC,
        LOG_ENTRY_SYNC,
        LEADERSHIP_LOST,
        CONNECTION_LOST,
        ERROR,  //Due to wrong version number
        UNKNOWN
    }

    LogReplicationTransportType transport;

    @Setter
    @Getter
    CrossSiteConfiguration crossSiteConfig;

    CorfuReplicationDiscoveryService discoveryService;

    //Setup while preparing a roletype change
    long prepareSiteRoleChangeStreamTail;

    long totalNumEntriesToSend;

    CorfuReplicationManager(LogReplicationTransportType transport, CrossSiteConfiguration crossSiteConfig, CorfuReplicationDiscoveryService discoveryService) {
        prepareSiteRoleChangeStreamTail = Address.NON_ADDRESS;
        totalNumEntriesToSend = 0;
        this.transport = transport;
        this.crossSiteConfig = crossSiteConfig;
        this.discoveryService = discoveryService;
    }


    /**
     * Connect and connect log replication to a remote site.
     *
     * * @throws InterruptedException
     */
    public void connect(LogReplicationNodeInfo localNode, CrossSiteConfiguration.SiteInfo remoteSite, CorfuReplicationDiscoveryService discoveryService) throws InterruptedException {

        log.trace("Setup runtime's from local node to remote site {}", remoteSite.getSiteId());

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    remoteSite.connect(localNode, transport, discoveryService);
                    LogReplicationNodeInfo leader = remoteSite.getRemoteLeader();
                    log.info("connect to site {} lead node {}:{}", remoteSite.getSiteId(), leader.getIpAddress(), leader.getPortNum());
                    remoteSiteRuntimeMap.put(remoteSite.getSiteId(), leader.getRuntime());
                } catch (Exception e) {
                    log.error("Exception {}.  Failed to connect to remote site {}. Retry after 1 second.",
                        e, remoteSite.getSiteId());
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
     * Once determined this is a Lead Sender (on primary site), connect log replication.
     */
    public void startLogReplication(LogReplicationNodeInfo localNode, String siteId, CorfuReplicationDiscoveryService discoveryService) {
        CrossSiteConfiguration.SiteInfo remoteSite = crossSiteConfig.getStandbySites().get(siteId);
        log.info("Start Log Replication to Standby Site {}", siteId);

        try {

            // a clean start up of the replication has done for this remote site
            if (remoteSiteRuntimeMap.get(siteId) != null) {
                return;
            }

            connect(localNode, remoteSite, discoveryService);

            CorfuLogReplicationRuntime runtime = remoteSiteRuntimeMap.get(siteId);

            //If we start from a stop state due to site switch over, we need to restart the consumer.
            runtime.getSourceManager().getLogReplicationFSM().startFSM(crossSiteConfig);


            //TODO: by xiaoqin
            LogReplicationEvent negotiationResult = startNegotiation(runtime);
            log.info("Log Replication Negotiation with {} result {}", siteId, negotiationResult);
            replicate(runtime, negotiationResult);

        } catch (Exception e) {
            log.error("Will stop this remote site replicaiton as caught an exception", e);
            //The remote runtime will be stopped and removed from the runtimeMap.
            stopLogReplication(siteId);
        }
    }


    /**
     * Stop the current runtime and reestablish runtimes and query the new leader.
     * @param localNode
     * @param siteId
     */
    public void restartLogReplication(LogReplicationNodeInfo localNode, String siteId, CorfuReplicationDiscoveryService discoveryService) {
        stopLogReplication(siteId);
        startLogReplication(localNode, siteId, discoveryService);
    }

    public void startLogReplication(LogReplicationNodeInfo nodeInfo, CorfuReplicationDiscoveryService discoveryService) {
        for (CrossSiteConfiguration.SiteInfo remoteSite : crossSiteConfig.getStandbySites().values()) {
            try {
                startLogReplication(nodeInfo, remoteSite.getSiteId(), discoveryService);
            } catch (Exception e) {
                log.error("Failed to start log replication to remote site {}", remoteSite.getSiteId());
                // TODO (if failed): put logic..
                // If failed against a standby, retry..
                //remove from the standby list as the discovery site will get notification of the change
                crossSiteConfig.getStandbySites().remove(remoteSite.getSiteId());
            }
        }
    }

    /**
     * The notification of change of adding/removing standbys without epoch change.
     * @param newConfig has the same siteConfigId as the current config
     */
    public void processStandbyChange(LogReplicationNodeInfo nodeInfo, CrossSiteConfiguration newConfig, CorfuReplicationDiscoveryService discoveryService) {
        if (newConfig.getSiteConfigID() != crossSiteConfig.getSiteConfigID()) {
            log.error("the new config {} doesn't have the same siteConfigId as the current one {}", newConfig, crossSiteConfig);
            return;
        }

        Map<String, CrossSiteConfiguration.SiteInfo> newStandbys = newConfig.getStandbySites();
        Map<String, CrossSiteConfiguration.SiteInfo> currentStandbys = crossSiteConfig.getStandbySites();
        newStandbys.keySet().retainAll(currentStandbys.keySet());
        Set<String> standbysToRemove = currentStandbys.keySet();
        standbysToRemove.removeAll(newStandbys.keySet());

        //Remove standbys that are not in the new config
        for (String siteID : standbysToRemove) {
            stopLogReplication(siteID);
            crossSiteConfig.removeStandbySite(siteID);
        }

        //Start the standbys that are in the new config but not in the current config
        for (String siteID : newConfig.getStandbySites().keySet()) {
            if (remoteSiteRuntimeMap.get(siteID) == null) {
                CrossSiteConfiguration.SiteInfo siteInfo = newConfig.getStandbySites().get(siteID);
                crossSiteConfig.addStandbySite(siteInfo);
                startLogReplication(nodeInfo, siteInfo.getSiteId(), discoveryService);
            }
        }
    }

    private void replicate(CorfuLogReplicationRuntime runtime, LogReplicationEvent negotiationResult) {
        switch (negotiationResult.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                log.info("Start Snapshot Sync Replication");
                runtime.startSnapshotSync();
                break;
            case SNAPSHOT_TRANSFER_COMPLETE:
                log.info("Should Start Snapshot Sync Phase II,but for now just restart full snapshot sync");
                // Right now it is hard to put logic for SNAPSHOT_TRANSFER_COMPLETE
                // replace it with SNAPSHOT_SYNC_REQUEST, and will re-examine it later.
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


    public void stopLogReplication() {
        for(String siteId: crossSiteConfig.getStandbySites().keySet()) {
            stopLogReplication(siteId);
        }
    }


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

        // Determine if we should proceed with Snapshot Sync or Log Entry Sync
        return processNegotiationResponse(negotiationResponse);

    }

    private LogReplicationEvent processNegotiationResponse(LogReplicationNegotiationResponse negotiationResponse)
            throws LogReplicationNegotiationException {

        log.info("Get negoti standby site state according to the response {}, will restart with a snapshot full sync event " ,
                negotiationResponse);

        // If the version are different, report an error.
        if (negotiationResponse.getVersion() != discoveryService.getLogReplicationMetadata().getVersion()) {
            log.error("The active site version {} is different from standby site version {}",
                    discoveryService.getLogReplicationMetadata().getVersion(), negotiationResponse.getVersion());
            throw new LogReplicationNegotiationException(" Mismatch of version number");
        }

        // The standby site has a smaller config ID, redo the discovery for this standby site when
        // getting a new notification of siteConfig change with a new added standby.
        if (negotiationResponse.getSiteConfigID() < negotiationResponse.getSiteConfigID()) {
            log.error("The active site configID {} is bigger than the standby configID {} ",
                    discoveryService.getLogReplicationMetadata().getSiteConfigID(), negotiationResponse.getSiteConfigID());
            throw new LogReplicationNegotiationException("Mismatch of configID");
        }

        // The standby site has larger config ID, redo the whole discovery for the active site when
        // getting a new notification of siteConig ID change.
        if (negotiationResponse.getSiteConfigID() > negotiationResponse.getSiteConfigID()) {
            log.error("The active site configID {} is smaller than the standby configID {} ",
                    discoveryService.getLogReplicationMetadata().getSiteConfigID(), negotiationResponse.getSiteConfigID());
            throw new LogReplicationNegotiationException("Mismatch of configID");
        }

        // Now the active and standby have the same version and same configID.

        // At the active site, get the current log head.
        CrossSiteConfiguration.SiteInfo siteInfo = crossSiteConfig.getStandbySites().values().iterator().next();
        CorfuRuntime runtime = siteInfo.getNodesInfo().get(0).getRuntime().getCorfuRuntime();
        long logHead = runtime.getAddressSpaceView().getTrimMark().getSequence();

        //It is a fresh start or it is in log entry sync state
        if (negotiationResponse.getSnapshotStart() == negotiationResponse.getSnapshotTransferred() &&
                negotiationResponse.getSnapshotStart() == negotiationResponse.getSnapshotApplied() &&
                negotiationResponse.getLastLogProcessed() >= negotiationResponse.getSnapshotStart()) {
            // If the next log entry is not trimmed, restart with log entry sync,
            // otherwise, start snapshot full sync.
            if (logHead <= negotiationResponse.getLastLogProcessed() + 1) {
                return new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_START,
                        new LogReplicationEventMetadata(LogReplicationEventMetadata.getNIL_UUID(), negotiationResponse.getLastLogProcessed()));
            } else {
                return new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
            }

        }

        //if it is in the snapshot full sync phase I, transferring data, restart the snapshot full sync
        if (negotiationResponse.getSnapshotStart() > negotiationResponse.getSnapshotTransferred()) {
            return new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
        }

        // If it is in the snapshot full sync phase II:
        // the data has been transferred to the standby site and the the standby site is applying data from shadow streams
        // to the real streams.
        // It doesn't need to transfer the data again, just send a SNAPSHOT_COMPLETE message to the standby site.
        if (negotiationResponse.getSnapshotStart() == negotiationResponse.getSnapshotTransferred() &&
                negotiationResponse.getSnapshotTransferred() > negotiationResponse.getSnapshotApplied()) {
            return new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE,
                    new LogReplicationEventMetadata(LogReplicationEventMetadata.getNIL_UUID(), negotiationResponse.getSnapshotStart()));
        }

        // For other scenarios, the standby site is in a wrong state, trigger a snapshot full sync.
        log.error("Could not recognize the standby site state according to the response {}, will restart with a snapshot full sync event " ,
                negotiationResponse);
        return new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
    }


    long queryStreamTail() {
        CrossSiteConfiguration.SiteInfo siteInfo = crossSiteConfig.getStandbySites().values().iterator().next();
        LogReplicationNodeInfo nodeInfo = siteInfo.getNodesInfo().get(0);
        return nodeInfo.getRuntime().getMaxStreamTail();
    }

    long queryEntriesToSent(long tail) {
        int totalNumEnries = 0;

        for (CorfuLogReplicationRuntime runtime: remoteSiteRuntimeMap.values()) {
            totalNumEnries += runtime.getNumEntriesToSend(tail);
        }

        return totalNumEnries;
    }

    /**
     * query the current all replication stream log tail and remeber the max
     * and query each standbySite information according to the ackInformation decide all manay total
     * msg needs to send out.
     */
    public void prepareSiteRoleChange() {
        prepareSiteRoleChangeStreamTail = queryStreamTail();
        totalNumEntriesToSend = queryEntriesToSent(prepareSiteRoleChangeStreamTail);
    }

    /**
     * query the current all replication stream log tail and calculate the number of messages to be sent.
     * If the max tail has changed, give 0 percent has done.
     * Percentage of work has been done, when it return 100, it has done the replication.
     */
    public int queryReplicationStatus() {
        long maxTail = queryStreamTail();

        //If the tail has been moved, reset the base calculation
        if (maxTail > prepareSiteRoleChangeStreamTail) {
            prepareSiteRoleChange();
        }

        long currentNumEntriesToSend = queryEntriesToSent(prepareSiteRoleChangeStreamTail);
        log.debug("maxTail " + maxTail + " totalNumEntriesToSend " + totalNumEntriesToSend + " currentNumEntriesToSend " + currentNumEntriesToSend);

        if (totalNumEntriesToSend == 0 || currentNumEntriesToSend == 0)
            return PERCENTAGE_BASE;

        //percentage of has been sent
        //as the currentNumEntriesToSend is not zero, the percent should not be 100%
        int percent = (int)((totalNumEntriesToSend - currentNumEntriesToSend)*PERCENTAGE_BASE/totalNumEntriesToSend);
        if (percent == PERCENTAGE_BASE) {
            percent = PERCENTAGE_BASE - 1;
        }

        return percent;
    }

    public void shutdown() {
        stopLogReplication();
    }
}
