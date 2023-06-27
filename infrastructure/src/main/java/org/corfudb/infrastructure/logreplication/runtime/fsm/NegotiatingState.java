package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationNegotiationException;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.infrastructure.logreplication.utils.SnapshotSyncUtils;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.view.Address;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationFsmUtil.canEnqueueStopRuntimeFsmEvent;

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

    private final CorfuLogReplicationRuntime fsm;

    private Optional<String> leaderNodeId;

    private final ThreadPoolExecutor worker;

    private final LogReplicationClientServerRouter router;

    private final LogReplicationMetadataManager metadataManager;

    public NegotiatingState(CorfuLogReplicationRuntime fsm, ThreadPoolExecutor worker,
                            LogReplicationClientServerRouter router, LogReplicationMetadataManager metadataManager) {
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
                    // If connection starter, router will attempt reconnection of non-leader endpoint
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
                leaderNodeId = Optional.empty();
                return fsm.getStates().get(LogReplicationRuntimeStateType.VERIFYING_REMOTE_LEADER);
            case REMOTE_LEADER_LOSS:
                if (fsm.getRemoteLeaderNodeId().get().equals(event.getNodeId())) {
                    fsm.resetRemoteLeaderNodeId();
                    return fsm.getStates().get(LogReplicationRuntimeStateType.VERIFYING_REMOTE_LEADER);
                }
                return null;
            case LOCAL_LEADER_LOSS:
                if (canEnqueueStopRuntimeFsmEvent(router, fsm, event.isConnectionStarter())) {
                    return fsm.getStates().get(LogReplicationRuntimeStateType.STOPPED);
                }
                return null;
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

                CorfuMessage.RequestPayloadMsg payload =
                        CorfuMessage.RequestPayloadMsg.newBuilder().setLrMetadataRequest(
                                LogReplication.LogReplicationMetadataRequestMsg.newBuilder().build()).build();
                CompletableFuture<LogReplicationMetadataResponseMsg> cf = router
                        .sendRequestAndGetCompletable(fsm.session, payload, remoteLeader);
                LogReplicationMetadataResponseMsg response =
                        cf.get(CorfuLogReplicationRuntime.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

                // Process Negotiation Response, and determine if we start replication and which type type to start
                // (snapshot or log entry sync). This will be carried along the negotiation_complete event.
                processNegotiationResponse(response);

                if(router.isConnectionStarterForSession(fsm.getSession())) {
                    // Negotiation to leader node completed, unblock channel in the router.
                    router.getSessionToLeaderConnectionFuture().get(fsm.getSession()).complete(null);
                }
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
     * It will decide to do a full snapshot sync or log entry sync according to the metadata received from the sink site.
     *
     * @param negotiationResponse
     * @return
     * @throws LogReplicationNegotiationException
     */
    private void processNegotiationResponse(LogReplicationMetadataResponseMsg negotiationResponse)
            throws LogReplicationNegotiationException {

        log.debug("Process negotiation response {} from {}", negotiationResponse, fsm.getRemoteClusterId());

        long topologyConfigId = metadataManager.getReplicationContext().getTopologyConfigId();

        /*
         * The sink site has a smaller config ID, redo the discovery for this sink site when
         * getting a new notification of the site config change if this sink is in the new config.
         */
        if (negotiationResponse.getTopologyConfigID() < topologyConfigId) {
            log.error("The source site configID {} is bigger than the sink configID {} ",
                topologyConfigId, negotiationResponse.getTopologyConfigID());
            throw new LogReplicationNegotiationException("Mismatch of configID");
        }

        /*
         * The sink site has larger config ID, redo the whole discovery for the source site
         * it will be triggered by a notification of the site config change.
         */
        if (negotiationResponse.getTopologyConfigID() > topologyConfigId) {
            log.error("The source site configID {} is smaller than the sink configID {} ",
                topologyConfigId, negotiationResponse.getTopologyConfigID());
            throw new LogReplicationNegotiationException("Mismatch of configID");
        }

        /*
         * Get the current log head.
         */
        long logHead = metadataManager.getLogHead();

        /*
         * It is a fresh start, start snapshot full sync.
         * Following is an example that metadata value indicates a fresh start, no replicated data at sink site:
         * "topologyConfigId": "10"
         * "version": "release-1.0"
         * "snapshotStart": "-1"
         * "snapshotSeqNum": "-1"
         * "snapshotTransferred": "-1"
         * "snapshotApplied": "-1"
         * "lastLogEntryProcessed": "-1"
         */
        if (negotiationResponse.getSnapshotStart() == Address.NON_ADDRESS) {
            log.info("No snapshot available in remote. Initiate SNAPSHOT sync to {}", fsm.getSession());
            startSnapshotSync();
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
            startSnapshotSync();
            return;
        }

        /*
         * If it is in the snapshot full sync transfer phase (Phase II):
         * the data has been transferred to the sink site and the the sink site is applying data from shadow streams
         * to the real streams.
         * It doesn't need to transfer the data again, just send a SNAPSHOT_COMPLETE message to the sink site.
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
         * An example to show the sink site is in log entry sync phase.
         * A full snapshot transfer based on timestamp 100 has been completed, and this sink has processed all log entries
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
                negotiationResponse.getLastLogEntryTimestamp() >= negotiationResponse.getSnapshotStart()) {
            /*
             * If the next log entry is not trimmed, restart with log entry sync,
             * otherwise, start snapshot full sync.
             */
            if (logHead <= negotiationResponse.getLastLogEntryTimestamp() + 1) {
                log.info("Resume LOG ENTRY sync. Address space has not been trimmed, deltas are guaranteed to be available. " +
                        "logHead={}, lastLogProcessed={}", logHead, negotiationResponse.getLastLogEntryTimestamp());
                fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE,
                        new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST,
                                new LogReplicationEventMetadata(LogReplicationEventMetadata.getNIL_UUID(), negotiationResponse.getLastLogEntryTimestamp(),
                                        negotiationResponse.getSnapshotApplied()))));
            } else {
                // TODO: it is OK for a first phase, but this might not be efficient/accurate, as the next (+1)
                //  might not really be the next entry (as that is a globalAddress and the +1 might not even belong to
                //  the stream to replicate). So we might be doing a Snapshot (full) sync when next entry really
                //  falls beyond the logHead. A more accurate approach would be to look for the next available entry
                //  in the the transaction stream.
                log.info(" Start SNAPSHOT sync. LOG ENTRY Sync cannot resume, address space has been trimmed." +
                        "logHead={}, lastLogProcessed={}", logHead, negotiationResponse.getLastLogEntryTimestamp());
                startSnapshotSync();
            }

            return;
        }

        // TODO(Future): consider continue snapshot sync from a remaining point (insert new event in LogReplicationFSM) -> efficiency

        /*
         * For other scenarios, the sink site is in a non-recognizable state, trigger a snapshot full sync.
         */
        log.warn("Could not recognize the sink cluster state according to the response {}, will restart with a snapshot full sync event" ,
                negotiationResponse);
        fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE,
                new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST)));
    }

    private void startSnapshotSync() {
        // If Routing Queue Replication Model, request LR Client to provide full sync data
        if (fsm.getSession().getSubscriber().getModel() == LogReplication.ReplicationModel.ROUTING_QUEUES) {
            SnapshotSyncUtils.enforceSnapshotSync(fsm.getSession(), new CorfuStore(metadataManager.getRuntime()),
                LogReplicationMetadata.ReplicationEvent.ReplicationEventType.RECEIVER_OUT_OF_SYNC_FORCE_SNAPSHOT_SYNC);
        }
        fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE,
            new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST)));
    }
}
