package org.corfudb.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.logreplication.runtime.LogReplicationRuntime;
import org.corfudb.logreplication.runtime.LogReplicationRuntimeParameters;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class CorfuReplicationManager {

    // Keep map of remote site endpoints and the associated log replication client
    Map<String, LogReplicationRuntime> logReplicationRuntimes = new HashMap<>();

    private final SiteToSiteConfiguration config;

    public CorfuReplicationManager(SiteToSiteConfiguration config) {
        this.config = config;
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
     * Once determined this is a Lead Sender (on primary site), start log replication.
     */
    public void startLogReplication() {

        String remoteLRSEndpoint = config.backupSite.getRemoteLeaderEndpoint();

        // Add Client to Remote Endpoint
        LogReplicationRuntime runtime = logReplicationRuntimes.computeIfAbsent(remoteLRSEndpoint, remote -> {
            LogReplicationRuntimeParameters parameters = LogReplicationRuntimeParameters.builder()
                    .localCorfuEndpoint(config.getLocalCorfuEndpoint())
                    .remoteLogReplicationServerEndpoint(config.getRemoteLogReplicationServer()).build();
            LogReplicationRuntime replicationRuntime = new LogReplicationRuntime(parameters);
            replicationRuntime.connect();
            return replicationRuntime;
        });

        // Initiate Log Replication Negotiation Protocol 
        LogReplicationNegotiationResult negotiationResult = startNegotiation(runtime);

        log.info("Log Replication Negotiation result {}", negotiationResult);

        switch (negotiationResult) {
            case SNAPSHOT_SYNC:
                runtime.startSnapshotSync();
                break;
            case LOG_ENTRY_SYNC:
                runtime.startLogEntrySync();
                break;
            case LEADERSHIP_LOST:
            case CONNECTION_LOST:
            case UNKNOWN:
                // Re-Trigger Discovery Leader Receiver
                break;
        }
    }


    /**
     * Once determined this is a Lead Receiver (on standby site), start log receivers.
     */
    public void startLogReceive() {
        //
    }

    private LogReplicationNegotiationResult startNegotiation(LogReplicationRuntime logReplicationRuntime) {
        try {
            log.info("Start Negotiation");
            LogReplicationNegotiationResponse negotiationResponse = logReplicationRuntime.startNegotiation();
            log.info("Negotiation Response received: " + negotiationResponse);
            // Process Negotiation Response and determine if we should proceed with Snapshot Sync or Log Entry Sync
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
