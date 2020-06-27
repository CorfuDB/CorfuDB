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
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class NegotiatingState implements LogReplicationRuntimeState {

    private CorfuLogReplicationRuntime fsm;

    private Optional<String> leaderEndpoint;

    private volatile AtomicBoolean inProgress = new AtomicBoolean(false);

    private ExecutorService worker;

    private LogReplicationClientRouter router;

    private LogReplicationMetadataManager metadataManager;

    public NegotiatingState(CorfuLogReplicationRuntime fsm, ExecutorService worker, LogReplicationClientRouter router,
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
                String endpointDown = event.getEndpoint();
                // Update list of valid connections.
                fsm.updateDisconnectedEndpoints(endpointDown);

                // If the leader is the node that become unavailable, verify new leader and attempt to reconnect.
                if (leaderEndpoint.equals(endpointDown)) {
                    leaderEndpoint = Optional.empty();
                    return fsm.getStates().get(LogReplicationRuntimeStateType.VERIFYING_REMOTE_LEADER);
                } else {
                    // Router will attempt reconnection of non-leader endpoint
                    return this;
                }
            case ON_CONNECTION_UP:
                // Some node got connected, update connected endpoints
                fsm.updateConnectedEndpoints(event.getEndpoint());
                return this;
            case NEGOTIATION_COMPLETE:
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
        // Start Negotiation (check if ongoing negotiation is in progress)
        if(!inProgress.get()) {
            // Start Negotiation
            worker.submit(this::negotiate);
        }
    }

    private void negotiate() {
        try {
            if(fsm.getLeader().isPresent()) {
                String remoteLeader = fsm.getLeader().get();
                CompletableFuture<LogReplicationNegotiationResponse> cf = router.sendMessageAndGetCompletable(
                        new CorfuMsg(CorfuMsgType.LOG_REPLICATION_NEGOTIATION_REQUEST).setEpoch(0), remoteLeader);
                LogReplicationNegotiationResponse response = cf.get(CorfuLogReplicationRuntime.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

                // Process Negotiation Response, and determine if we start replication and which type type to start
                // (snapshot or log entry sync). This will be carried along the negotiation_complete event.
                processNegotiationResponse(response);

                // Negotiation to leader node completed, unblock channel in the router.
                router.getConnectionFuture().complete(null);

                // Negotiation completed
                inProgress.set(false);
            } else {
                // No leader found at the time of negotiation
                fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS));
            }
        } catch (LogReplicationNegotiationException ne) {
            log.error("Negotiation request timed out. Retry, until connection is marked as down or recovers.", ne);
            fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.ERROR));
        } catch (TimeoutException te) {
            log.error("Negotiation request timed out. Retry, until connection is marked as down or recovers.", te);
            fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_FAILED));
        } catch (Exception e) {
            log.error("Unexpected exception during negotiation, retry.", e);
            fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_FAILED));
        }
    }

    /**
     * Set Leader Endpoint, determined during the transition from VERIFYING_REMOTE_LEADER
     * to NEGOTIATING state.
     *
     * @param endpoint leader node on remote cluster
     */
    public void setLeaderEndpoint(String endpoint) {
        this.leaderEndpoint = Optional.of(endpoint);
    }

    /**
     * It will decide to do a full snapshot sync or log entry sync according to the metadata received from the standby site.
     *
     * @param negotiationResponse
     * @return
     * @throws LogReplicationNegotiationException
     */
    private void processNegotiationResponse(LogReplicationNegotiationResponse negotiationResponse)
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
         * Now the active and standby have the same version and same configID.
         */

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
         * "snapshotSeqNum": " -1"
         * "snapshotTransferred": "-1"
         * "snapshotApplied": "-1"
         * "lastLogEntryProcessed": "-1"
         */
        if (negotiationResponse.getSnapshotStart() == -1) {
            negotiationResponse.getLastLogProcessed();
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
         * "snapshotSeqNum": " 88"
         * "snapshotTransferred": "-1"
         * "snapshotApplied": "-1"
         * "lastLogEntryProcessed": "-1"
         */
        if (negotiationResponse.getSnapshotStart() > negotiationResponse.getSnapshotTransferred()) {
            log.info("Get the negotiation response {} and will start replication.",
                    negotiationResponse);
            fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE,
                    new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST)));
            return;
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
            fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE,
                    new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_WAIT_COMPLETE,
                            new LogReplicationEventMetadata(LogReplicationEventMetadata.getNIL_UUID(), negotiationResponse.getSnapshotStart()))));
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
                fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE,
                        new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_START,
                                new LogReplicationEventMetadata(LogReplicationEventMetadata.getNIL_UUID(), negotiationResponse.getLastLogProcessed())
                        )));
            } else {
                fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE,
                        new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST)));
            }

            return;
        }

        /*
         * For other scenarios, the standby site is in a non-recognizable state, trigger a snapshot full sync.
         */
        log.warn("Could not recognize the standby cluster state according to the response {}, will restart with a snapshot full sync event " ,
                negotiationResponse);
        fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE,
                new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST)));
    }
}
