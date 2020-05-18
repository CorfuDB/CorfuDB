package org.corfudb.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.runtime.LogReplicationRuntime;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class CorfuReplicationManager {

    // Keep map of remote site endpoints and the associated log replication client
    Map<String, LogReplicationRuntime> logReplicationRuntimes = new HashMap<>();

    enum LogReplicationNegotiationResult {
        SNAPSHOT_SYNC,
        LOG_ENTRY_SYNC,
        LEADERSHIP_LOST,
        CONNECTION_LOST,
        ERROR,
        UNKNOWN
    }

    public void setupReplicationLeaderRuntime(CrossSiteConfiguration.NodeInfo nodeInfo, CrossSiteConfiguration config)
            throws InterruptedException {
        log.info("setupReplicationLeaderRuntime config {}", config);

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    for (Map.Entry<String, CrossSiteConfiguration.SiteInfo> entry : config.getStandbySites().entrySet()) {
                        entry.getValue().setupLogReplicationRemoteRuntime(nodeInfo, config.getEpoch());
                        log.info("setupReplicationLeaderRuntime {}", entry);
                        CrossSiteConfiguration.NodeInfo leader = entry.getValue().getRemoteLeader();
                        logReplicationRuntimes.put(entry.getKey(), leader.runtime);
                    }
                } catch (Exception e) {
                    log.error("Failed to connect to a remote site. Retry after 1 second.");
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
     * Once determined this is a Lead Sender (on primary site), start log replication.
     */
    public void startLogReplication(CrossSiteConfiguration config) {
        log.info("Start Log Replication to Standby Sites {}", logReplicationRuntimes.keySet());
        for(Map.Entry<String, LogReplicationRuntime> entry: logReplicationRuntimes.entrySet()) {
            String endpoint = entry.getKey();
            LogReplicationRuntime runtime = entry.getValue();

            LogReplicationNegotiationResult negotiationResult = startNegotiation(runtime);
            log.info("Log Replication Negotiation with {} result {}", endpoint, negotiationResult);
            startReplication(config.getEpoch(), runtime, negotiationResult);
            runtime.getSourceManager().getLogReplicationFSM().startConsumer(config);
        }
    }

    public void stopLogReplication(CrossSiteConfiguration config) {
        System.out.print("Log Replication stop " + config);

        for(Map.Entry<String, LogReplicationRuntime> entry: logReplicationRuntimes.entrySet()) {
            LogReplicationRuntime runtime = entry.getValue();
            runtime.stop();
        }
    }

    private void startReplication(long siteEpoch, LogReplicationRuntime runtime, LogReplicationNegotiationResult negotiationResult) {
        runtime.getSourceManager().getLogReplicationFSM().setSiteEpoch(siteEpoch);

        switch (negotiationResult) {
            case SNAPSHOT_SYNC:
                log.info("Start Snapshot Sync Replication");
                //System.out.print("\nStart Snapshot Sync Replication ");
                runtime.startSnapshotSync();
                break;
            case LOG_ENTRY_SYNC:
                log.info("Start Log Entry Sync Replication");
                //System.out.print("\nStart Log Entry Sync Replication");
                runtime.startLogEntrySync();
                break;
            case LEADERSHIP_LOST:
            case CONNECTION_LOST:
            case UNKNOWN:
                log.info("Invalid Negotiation result. Re-trigger discovery.");
                //System.out.print("\nInvalid Negotiation result. Re-trigger discovery.");
                // Re-Trigger Discovery Leader Receiver
                break;
        }
    }

    private LogReplicationNegotiationResult startNegotiation(LogReplicationRuntime logReplicationRuntime) {
        try {
            log.info("Start Negotiation");
            LogReplicationNegotiationResponse negotiationResponse = logReplicationRuntime.startNegotiation();
            log.info("Negotiation Response received: " + negotiationResponse);
            // Determine if we should proceed with Snapshot Sync or Log Entry Sync
            return processNegotiationResponse(negotiationResponse);
        } catch (Exception e) {
            log.error("Caught an exception during log replication negotiation", e);
            return LogReplicationNegotiationResult.ERROR;
        }
    }

    private LogReplicationNegotiationResult processNegotiationResponse(LogReplicationNegotiationResponse negotiationResponse) {
        // TODO (TEMP): for now default always to snapshot sync
        return LogReplicationNegotiationResult.SNAPSHOT_SYNC;
    }
}
