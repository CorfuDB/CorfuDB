package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.LogReplicationTransportType;
import org.corfudb.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class CorfuReplicationManager {

    // Keep map of remote site endpoints and the associated log replication runtime (client)
    Map<String, CorfuLogReplicationRuntime> remoteSiteRuntimeMap = new HashMap<>();

    enum LogReplicationNegotiationResult {
        SNAPSHOT_SYNC,
        LOG_ENTRY_SYNC,
        LEADERSHIP_LOST,
        CONNECTION_LOST,
        ERROR,
        UNKNOWN
    }

    LogReplicationTransportType transport;

    @Setter
    @Getter
    CrossSiteConfiguration crossSiteConfig;

    CorfuReplicationManager(LogReplicationTransportType transport, CrossSiteConfiguration crossSiteConfig) {
        this.transport = transport;
        this.crossSiteConfig = crossSiteConfig;
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
                    log.error("Failed to connect to remote sit {}. Retry after 1 second.", remoteSite.getSiteId());
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

            LogReplicationNegotiationResult negotiationResult = startNegotiation(runtime);
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

    private void replicate(CorfuLogReplicationRuntime runtime, LogReplicationNegotiationResult negotiationResult) {
            switch (negotiationResult) {
            case SNAPSHOT_SYNC:
                log.info("Start Snapshot Sync Replication");
                runtime.startSnapshotSync();
                break;
            case LOG_ENTRY_SYNC:
                log.info("Start Log Entry Sync Replication");
                runtime.startLogEntrySync();
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


    private LogReplicationNegotiationResult startNegotiation(CorfuLogReplicationRuntime logReplicationRuntime)
            throws LogReplicationNegotiationException {

        LogReplicationNegotiationResponse negotiationResponse;

        try {
            // TODO(Anny) : IRetry...
            negotiationResponse = logReplicationRuntime.startNegotiation();
            log.trace("Negotiation Response received: {} ", negotiationResponse);
        } catch (Exception e) {
            log.error("Caught exception during log replication negotiation to {}", logReplicationRuntime.getParameters().getRemoteLogReplicationServerEndpoint());
            throw new LogReplicationNegotiationException(LogReplicationNegotiationResult.UNKNOWN);
        }

        // Determine if we should proceed with Snapshot Sync or Log Entry Sync
        return processNegotiationResponse(negotiationResponse);

    }

    private LogReplicationNegotiationResult processNegotiationResponse(LogReplicationNegotiationResponse negotiationResponse)
            throws LogReplicationNegotiationException {

        // TODO (Anny): for now default always to snapshot sync
        return LogReplicationNegotiationResult.SNAPSHOT_SYNC;
    }
}
