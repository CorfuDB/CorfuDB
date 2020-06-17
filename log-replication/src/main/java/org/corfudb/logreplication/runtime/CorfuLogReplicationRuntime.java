package org.corfudb.logreplication.runtime;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.cluster.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.receive.LogReplicationMetadataManager;
import org.corfudb.logreplication.infrastructure.LogReplicationNegotiationException;
import org.corfudb.logreplication.send.LogReplicationEventMetadata;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.transport.logreplication.LogReplicationClientRouter;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.logreplication.LogReplicationSourceManager;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;

import java.util.UUID;

/**
 * Client Runtime to connect to a Corfu Log Replication Server.
 *
 * The Log Replication Server is currently conformed uniquely by
 * a Log Replication Unit. This client runtime is a wrapper around all
 * units.
 *
 * @author amartinezman
 */
@Slf4j
public class CorfuLogReplicationRuntime {
    /**
     * The parameters used to configure this {@link CorfuLogReplicationRuntime}.
     */
    private final LogReplicationRuntimeParameters parameters;

    /**
     * Log Replication Client Router
     */
    private LogReplicationClientRouter router;

    /**
     * Log Replication Client
     */
    private LogReplicationClient client;

    /**
     * Log Replication Source Manager (producer of log updates)
     */
    private LogReplicationSourceManager sourceManager;

    /**
     * Log Replication Configuration
     */
    private LogReplicationConfig logReplicationConfig;

    private LogReplicationMetadataManager metadataManager;

    private final CorfuRuntime corfuRuntime;

    private final long topologyConfigId;

    /**
     * Constructor
     *
     * @param parameters runtime parameters
     */
    public CorfuLogReplicationRuntime(@NonNull LogReplicationRuntimeParameters parameters,
                                      @NonNull LogReplicationMetadataManager metadataManager,
                                      long topologyConfigId) {
        this.parameters = parameters;
        this.logReplicationConfig = parameters.getReplicationConfig();
        this.metadataManager = metadataManager;
        this.topologyConfigId = topologyConfigId;

        corfuRuntime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(parameters.getLocalCorfuEndpoint());
        corfuRuntime.connect();
    }

    /**
     * Set the transport layer for communication to remote cluster.
     *
     * The transport layer encompasses:
     *
     * - Clients to direct remote counter parts (handles specific funcionalities)
     * - A router which will manage different clients (currently
     * we only communicate to the Log Replication Server)
     */
    private void configureChannel() {

        ClusterDescriptor remoteSite = parameters.getRemoteClusterDescriptor();
        String remoteClusterId = remoteSite.getClusterId();

        log.info("Configure transport to remote cluster {}", remoteClusterId);

        // This Router is more of a forwarder (and client manager), as it is not specific to a remote node,
        // but instead valid to all nodes in a remote cluster. Routing to a specific node (endpoint)
        // is the responsibility of the transport layer in place (through the adapter).
        router = new LogReplicationClientRouter(remoteSite, parameters, this::onNegotiation);
        router.addClient(new LogReplicationHandler());
        router.connect();
        // Create all clients to remote cluster components (currently only one server component is supported, Log Replication Unit)
        client = new LogReplicationClient(router, remoteSite.getClusterId());
    }

    /**
     * Connect to remote site.
     **/
    public void connect() {
        configureChannel();
        log.info("Connected. Set Source Manager to connect to local Corfu on {}", parameters.getLocalCorfuEndpoint());
        sourceManager = new LogReplicationSourceManager(parameters.getLocalCorfuEndpoint(),
                    client, logReplicationConfig);
    }

    public LogReplicationNegotiationResponse startNegotiation() throws Exception {
        log.info("Send Negotiation Request");
        return client.sendNegotiationRequest().get();
    }

    /**
     * Start snapshot (full) sync
     */
    private void startSnapshotSync() {
        UUID snapshotSyncRequestId = sourceManager.startSnapshotSync();
        log.info("Start Snapshot Sync[{}]", snapshotSyncRequestId);
    }

    /**
     * Start log entry (delta) sync
     */
    private void startLogEntrySync(LogReplicationEvent event) {
        log.info("Start Log Entry Sync");
        sourceManager.startReplication(event);
    }

    /**
     * Clean up router, stop source manager.
     */
    public void stop() {
        router.stop();
        sourceManager.shutdown();
    }

    /**
     *
     *
     * @param ts
     * @return
     */
    public long getNumEntriesToSend(long ts) {
        long ackTS = sourceManager.getLogReplicationFSM().getAckedTimestamp();
        return ts - ackTS;
    }

    /**
     * Runtime callback on negotiation
     */
    public void onNegotiation(LogReplicationNegotiationResponse negotiationResponse) {
        try {
            LogReplicationEvent replicationEvent = processNegotiationResponse(negotiationResponse);
            startLogReplication(replicationEvent);
        } catch (LogReplicationNegotiationException e) {
            // Cannot proceed to log replication, as negotiation did not complete successfully
        }
    }

    /**
     * Start replication process by put replication event to FSM.
     *
     * @param negotiationResult
     */
    private void startLogReplication(LogReplicationEvent negotiationResult) {
        // If we start from a stop state due to cluster switch over, we need to restart the consumer.
        sourceManager.getLogReplicationFSM().startFSM(topologyConfigId);

        switch (negotiationResult.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                log.info("Start Snapshot Sync Replication");
                startSnapshotSync();
                break;
            case SNAPSHOT_WAIT_COMPLETE:
                log.info("Should Start Snapshot Sync Phase II,but for now just restart full snapshot sync");
                startSnapshotSync();
                break;
            case REPLICATION_START:
                log.info("Start Log Entry Sync Replication");
                startLogEntrySync(negotiationResult);
                break;
            default:
                log.info("Invalid Negotiation result. Re-trigger discovery.");
                break;
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
        if (!negotiationResponse.getVersion().equals(metadataManager.getVersion())) {
            log.error("The active site version {} is different from standby site version {}",
                    metadataManager.getVersion(), negotiationResponse.getVersion());
            throw new LogReplicationNegotiationException(" Mismatch of version number");
        }

        /*
         * The standby site has a smaller config ID, redo the discovery for this standby site when
         * getting a new notification of the site config change if this standby is in the new config.
         */
        if (negotiationResponse.getSiteConfigID() < negotiationResponse.getSiteConfigID()) {
            log.error("The active site configID {} is bigger than the standby configID {} ",
                    metadataManager.getSiteConfigID(), negotiationResponse.getSiteConfigID());
            throw new LogReplicationNegotiationException("Mismatch of configID");
        }

        /*
         * The standby site has larger config ID, redo the whole discovery for the active site
         * it will be triggered by a notification of the site config change.
         */
        if (negotiationResponse.getSiteConfigID() > negotiationResponse.getSiteConfigID()) {
            log.error("The active site configID {} is smaller than the standby configID {} ",
                    metadataManager.getSiteConfigID(), negotiationResponse.getSiteConfigID());
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
         * "snapshotTransferred": "-1"
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
         * "snapshotTransferred": "-1"
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
         * "snapshotTransferred": "100"
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
         * "snapshotTransferred": "100"
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

}
