package org.corfudb.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.LogReplicationTransportType;
import org.corfudb.logreplication.runtime.LogReplicationRuntime;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class CorfuReplicationManager {

    // Keep map of remote site endpoints and the associated log replication runtime (client)
    Map<String, LogReplicationRuntime> remoteSiteRuntimeMap = new HashMap<>();

    public CorfuReplicationManager() {
    }

    enum LogReplicationNegotiationResult {
        SNAPSHOT_SYNC,
        LOG_ENTRY_SYNC,
        LEADERSHIP_LOST,
        CONNECTION_LOST,
        ERROR,
        UNKNOWN
    }

    /**
     * Connect and connect log replication to a remote site.
     *
     * * @throws InterruptedException
     */
    public void connect(CrossSiteConfiguration.NodeInfo localNode, CrossSiteConfiguration.Site remoteSite,
                        LogReplicationTransportType transport) throws InterruptedException {

        log.trace("Setup runtime's from local node to remote site {}", remoteSite.getSiteId());

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    remoteSite.connect(localNode, transport);
                    CrossSiteConfiguration.NodeInfo leader = remoteSite.getRemoteLeader();
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
    public void startLogReplication(String siteId) throws LogReplicationNegotiationException {
        log.info("Start Log Replication to Standby Site {}", siteId);
        LogReplicationRuntime runtime = remoteSiteRuntimeMap.get(siteId);
        LogReplicationNegotiationResult negotiationResult = startNegotiation(runtime);
        log.info("Log Replication Negotiation with {} result {}", siteId, negotiationResult);
        replicate(runtime, negotiationResult);
    }

    private void replicate(LogReplicationRuntime runtime, LogReplicationNegotiationResult negotiationResult) {

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

    private LogReplicationNegotiationResult startNegotiation(LogReplicationRuntime logReplicationRuntime)
            throws LogReplicationNegotiationException {

        LogReplicationNegotiationResponse negotiationResponse;

        try {
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
