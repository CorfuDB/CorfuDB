package org.corfudb.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.logreplication.runtime.LogReplicationRuntime;
import org.corfudb.logreplication.runtime.LogReplicationRuntimeParameters;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class CorfuReplicationManager {

    // Keep map of remote site endpoints and the associated log replication client
    Map<String, LogReplicationRuntime> logReplicationRuntimes = new HashMap<>();

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
     * Once determined this is a Lead Sender (on primary site), start log replication.
     */
    public void startLogReplication(CrossSiteConfiguration config) {

        Map<String, String> remoteLRSEndpoints = config.getRemoteLeaderEndpoints();

        log.info("Start log replication to remote site endpoints {}", remoteLRSEndpoints);

        // Add Client to Remote Endpoint
        for (Map.Entry<String, String> remoteSiteToEndpoint : remoteLRSEndpoints.entrySet()) {
            String remoteSiteId = remoteSiteToEndpoint.getKey();
            String remoteSiteEndpoint = remoteSiteToEndpoint.getValue();
            log.info("Initialize runtime to {} in site {}", remoteSiteEndpoint, remoteSiteId);

            LogReplicationRuntime runtime;

            if(logReplicationRuntimes.get(remoteSiteId) != null) {
                // If runtime exists for this remote site, verify the lead endpoint has not changed
                LogReplicationRuntime rt = logReplicationRuntimes.get(remoteSiteId);
                if (!rt.getParameters().getRemoteLogReplicationServerEndpoint().equals(remoteSiteEndpoint)) {
                    // If remote lead endpoint changed
                    // Todo: some shutdown / disconnect on the previous runtime
                    // rt.disconnect();
                    logReplicationRuntimes.remove(remoteSiteId);
                }
            }

            runtime = logReplicationRuntimes.computeIfAbsent(remoteSiteId, remote -> {
                    LogReplicationRuntimeParameters parameters = LogReplicationRuntimeParameters.builder()
                            .localCorfuEndpoint(config.getLocalCorfuEndpoint())
                            .remoteLogReplicationServerEndpoint(remoteSiteEndpoint).build();
                    LogReplicationRuntime replicationRuntime = new LogReplicationRuntime(parameters);
                    replicationRuntime.connect();
                    return replicationRuntime;
                });

            // Initiate Log Replication Negotiation Protocol
            LogReplicationNegotiationResult negotiationResult = startNegotiation(runtime);

            log.info("Log Replication Negotiation with {} result {}", remoteSiteEndpoint, negotiationResult);

            startReplication(runtime, negotiationResult);
        }
    }

    private void startReplication(LogReplicationRuntime runtime, LogReplicationNegotiationResult negotiationResult) {

        switch (negotiationResult) {
            case SNAPSHOT_SYNC:
                log.info("Start Snapshot Sync Replication");
                runtime.startSnapshotSync();
                break;
            case LOG_ENTRY_SYNC:
                log.info("Start Log Entry Sync Replication");
                runtime.startLogEntrySync();
                break;
            case LEADERSHIP_LOST:
            case CONNECTION_LOST:
            case UNKNOWN:
                log.info("Invalid Negotiation result. Re-trigger discvoery.");
                // Re-Trigger Discovery Leader Receiver
                break;
        }
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
