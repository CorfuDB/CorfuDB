package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationNegotiationException;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationMetadataResponse;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Log Replication Runtime Negotiating State.
 *
 * During this state, replication is being negotiated between source and sink leaders
 * in an effort to determine the starting point of the log replication.
 *
 * @author amartinezman
 */
@Slf4j
public class NegotiatingState implements LogReplicationRuntimeState {

    private CorfuLogReplicationRuntime fsm;

    private Optional<String> leaderNodeId;

    private ThreadPoolExecutor worker;

    private LogReplicationClientRouter router;

    private LogReplicationMetadataManager metadataManager;

    public NegotiatingState(CorfuLogReplicationRuntime fsm, ThreadPoolExecutor worker, LogReplicationClientRouter router,
                            LogReplicationMetadataManager metadataManager) {
        this.fsm = fsm;
        this.metadataManager = metadataManager;
        this.worker = worker;
        this.router = router;
    }

    @Override
    public LogReplicationRuntimeStateType getType() {
        return LogReplicationRuntimeStateType.NEGOTIATING;
    }

    @Override
    public LogReplicationRuntimeState processEvent(LogReplicationRuntimeEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case ON_CONNECTION_DOWN:
                String nodeIdDown = event.getNodeId();
                // Update list of valid connections.
                fsm.updateDisconnectedNodes(nodeIdDown);

                // If the leader is the node that become unavailable, verify new leader and attempt to reconnect.
                if (leaderNodeId.isPresent() && leaderNodeId.get().equals(nodeIdDown)) {
                    leaderNodeId = Optional.empty();
                    return fsm.getStates().get(LogReplicationRuntimeStateType.VERIFYING_REMOTE_LEADER);
                } else {
                    // Router will attempt reconnection of non-leader endpoint
                    return null;
                }
            case ON_CONNECTION_UP:
                // Some node got connected, update connected endpoints
                fsm.updateConnectedNodes(event.getNodeId());
                return null;
            case NEGOTIATION_COMPLETE:
                log.info("Negotiation complete, result={}", event.getNegotiationResult());
                ((ReplicatingState)fsm.getStates().get(LogReplicationRuntimeStateType.REPLICATING)).setReplicationEvent(event.getNegotiationResult());
                return fsm.getStates().get(LogReplicationRuntimeStateType.REPLICATING);
            case NEGOTIATION_FAILED:
                return this;
            case REMOTE_LEADER_NOT_FOUND:
                return fsm.getStates().get(LogReplicationRuntimeStateType.VERIFYING_REMOTE_LEADER);
            case LOCAL_LEADER_LOSS:
                return fsm.getStates().get(LogReplicationRuntimeStateType.STOPPED);
            case ERROR:
                ((UnrecoverableState)fsm.getStates().get(LogReplicationRuntimeStateType.UNRECOVERABLE)).setThrowableCause(event.getT().getCause());
                return fsm.getStates().get(LogReplicationRuntimeStateType.UNRECOVERABLE);
            default: {
                log.warn("Unexpected communication event {} when in init state.", event.getType());
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(LogReplicationRuntimeState from) {
        log.debug("OnEntry :: negotiating state from {}", from.getType());
        log.trace("Submitted tasks to worker :: size={} activeCount={} taskCount={}", worker.getQueue().size(),
                worker.getActiveCount(), worker.getTaskCount());
        worker.submit(this::negotiate);
    }

    private void negotiate() {

        log.debug("Enter :: negotiate");

        try {
            if(fsm.getRemoteLeaderNodeId().isPresent()) {
                String remoteLeader = fsm.getRemoteLeaderNodeId().get();
                CompletableFuture<LogReplicationMetadataResponse> cf = router.sendMessageAndGetCompletable(
                        new CorfuMsg(CorfuMsgType.LOG_REPLICATION_METADATA_REQUEST).setEpoch(0), remoteLeader);
                LogReplicationMetadataResponse response = cf.get(CorfuLogReplicationRuntime.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

                // Process Negotiation Response, and determine if we start replication and which type type to start
                // (snapshot or log entry sync). This will be carried along the negotiation_complete event.
                processNegotiationResponse(response);

                // Negotiation to leader node completed, unblock channel in the router.
                router.getRemoteLeaderConnectionFuture().complete(null);
            } else {
                log.debug("No leader found during negotiation.");
                // No leader found at the time of negotiation
                fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS));
            }
        } catch (LogReplicationNegotiationException | TimeoutException ex) {
            log.error("Negotiation failed. Retry, until negotiation succeeds or connection is marked as down.", ex);
            fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_FAILED));
        } catch (Exception e) {
            log.error("Unexpected exception during negotiation, retry.", e);
            fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_FAILED));
        } finally {
            log.debug("Exit :: negotiate");
        }
    }

    /**
     * Set Leader Endpoint, determined during the transition from VERIFYING_REMOTE_LEADER
     * to NEGOTIATING state.
     *
     * @param nodeId leader node on remote cluster
     */
    public void setLeaderNodeId(String nodeId) {
        this.leaderNodeId = Optional.of(nodeId);
    }

    /**
     * It will decide to do a full snapshot sync or log entry sync according to the metadata received from the standby site.
     *
     * @param negotiationResponse
     * @return
     * @throws LogReplicationNegotiationException
     */
    private void processNegotiationResponse(LogReplicationMetadataResponse negotiationResponse)
            throws LogReplicationNegotiationException {

        log.debug("Process negotiation response {} from {}", negotiationResponse, fsm.getRemoteClusterId());

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
        if (negotiationResponse.getTopologyConfigId() < metadataManager.getTopologyConfigId()) {
            log.error("The active site configID {} is bigger than the standby configID {} ",
                    metadataManager.getTopologyConfigId(), negotiationResponse.getTopologyConfigId());
            throw new LogReplicationNegotiationException("Mismatch of configID");
        }

        /*
         * The standby site has larger config ID, redo the whole discovery for the active site
         * it will be triggered by a notification of the site config change.
         */
        if (negotiationResponse.getTopologyConfigId() > metadataManager.getTopologyConfigId()) {
            log.error("The active site configID {} is smaller than the standby configID {} ",
                    metadataManager.getTopologyConfigId(), negotiationResponse.getTopologyConfigId());
            throw new LogReplicationNegotiationException("Mismatch of configID");
        }

        /*
         * Get the current log head.
         */
        long logHead = metadataManager.getLogHead();

        /*
         * It is a fresh start, start snapshot full sync.
         * Following is an example that metadata value indicates a fresh start, no replicated data at standby site:
         * "topologyConfigId": "10"
         * "version": "release-1.0"
         * "snapshotStart": "-1"
         * "snapshotSeqNum": "-1"
         * "snapshotTransferred": "-1"
         * "snapshotApplied": "-1"
         * "lastLogEntryProcessed": "-1"
         */
        if (negotiationResponse.getSnapshotStart() == -1) {
            log.info("No snapshot available in remote. Initiate SNAPSHOT sync to {}", fsm.getRemoteClusterId());
            fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE,
                    new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST)));
            return;
        }

        /*
         * If it is in the snapshot full sync phase I, transferring data, restart the snapshot full sync.
         * An example of in Snapshot Sync Phase I, transfer phase:
         * "topologyConfigId": "10"
         * "version": "release-1.0"
         * "snapshotStart": "100"
         * "snapshotSeqNum": "88"
         * "snapshotTransferred": "-1"
         * "snapshotApplied": "-1"
         * "lastLogEntryProcessed": "-1"
         */
        if (negotiationResponse.getSnapshotStart() > negotiationResponse.getSnapshotTransferred()) {
            log.info("Previous Snapshot Sync transfer did not complete. Restart SNAPSHOT sync, snapshotStart={}, snapshotTransferred={}",
                    negotiationResponse.getSnapshotStart(), negotiationResponse.getSnapshotTransferred());
            fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE,
                    new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST)));
            return;
        }

        /*
         * If it is in the snapshot full sync transfer phase (Phase II):
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
            log.info("Previous Snapshot Sync transfer complete. Apply in progress, wait. snapshotStart={}, " +
                            "snapshotTransferred={}, snapshotApply={}", negotiationResponse.getSnapshotStart(),
                    negotiationResponse.getSnapshotTransferred(), negotiationResponse.getSnapshotApplied());
            fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE,
                    new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE,
                            new LogReplicationEventMetadata(LogReplicationEventMetadata.getNIL_UUID(), negotiationResponse.getSnapshotStart(),
                                    negotiationResponse.getSnapshotTransferred()))));
            return;
        }

        /* If it is in log entry sync state, continues log entry sync state.
         * An example to show the standby site is in log entry sync phase.
         * A full snapshot transfer based on timestamp 100 has been completed, and this standby has processed all log entries
         * between 100 to 200. A log entry sync should be restart if log entry 201 is not trimmed.
         * Otherwise, start a full snapshot full sync.
         * "topologyConfigId": "10"
         * "version": "release-1.0"
         * "snapshotStart": "100"
         * "snapshotSeqNum": "88"
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
                log.info("Resume LOG ENTRY sync. Address space has not been trimmed, deltas are guaranteed to be available. " +
                        "logHead={}, lastLogProcessed={}", logHead, negotiationResponse.getLastLogProcessed());
                fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE,
                        new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST,
                                new LogReplicationEventMetadata(LogReplicationEventMetadata.getNIL_UUID(), negotiationResponse.getLastLogProcessed(),
                                        negotiationResponse.getSnapshotApplied()))));
            } else {
                // TODO: it is OK for a first phase, but this might not be efficient/accurate, as the next (+1)
                //  might not really be the next entry (as that is a globalAddress and the +1 might not even belong to
                //  the stream to replicate). So we might be doing a Snapshot (full) sync when next entry really
                //  falls beyond the logHead. A more accurate approach would be to look for the next available entry
                //  in the the transaction stream.
                log.info(" Start SNAPSHOT sync. LOG ENTRY Sync cannot resume, address space has been trimmed." +
                        "logHead={}, lastLogProcessed={}", logHead, negotiationResponse.getLastLogProcessed());
                fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE,
                        new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST)));
            }

            return;
        }

        // TODO(Future): consider continue snapshot sync from a remaining point (insert new event in LogReplicationFSM) -> efficiency

        /*
         * For other scenarios, the standby site is in a non-recognizable state, trigger a snapshot full sync.
         */
        log.warn("Could not recognize the standby cluster state according to the response {}, will restart with a snapshot full sync event" ,
                negotiationResponse);
        fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE,
                new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST)));
    }
}
