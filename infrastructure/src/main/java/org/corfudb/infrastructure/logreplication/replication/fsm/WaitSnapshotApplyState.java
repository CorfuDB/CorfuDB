package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.LogReplication.SyncType;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class represents the WaitSnapshotApply state of the Log Replication State Machine.
 *
 * In this state the remote cluster is queried to verify snapshot sync has been applied
 * and move onto log entry sync (incremental update replication).
 *
 * This state is an optimization such that snapshot sync is separated into transfer and apply phases.
 * If data has been completely transferred and some failure occurs immediately after, the receiver can still
 * recover and data does not need to be transferred all over again.
 */
@Slf4j
public class WaitSnapshotApplyState implements LogReplicationState {

    /**
     * Delay in milliseconds to monitor replication status on receiver, when snapshot sync apply is in progress.
     */
    private static final int SCHEDULE_APPLY_MONITOR_DELAY = 2000;

    /**
     * Log Replication Finite State Machine Instance
     */
    private final LogReplicationFSM fsm;

    /**
     * Uniquely identifies the snapshot sync to which this wait state is associated.
     * This is required to validate if the incoming FSM event is for the current sync.
     */
    private UUID transitionSyncId;

    /**
     * Route query metadata messages to the remote cluster
     */
    private final DataSender dataSender;

    /**
     * Base Snapshot Timestamp for current Snapshot Sync
     */
    private long baseSnapshotTimestamp;

    /**
     * Constructor
     *
     * @param logReplicationFSM log replication state machine
     */
    public WaitSnapshotApplyState(LogReplicationFSM logReplicationFSM, DataSender dataSender) {
        this.fsm = logReplicationFSM;
        this.dataSender = dataSender;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                log.info("[{}]:: Snapshot Sync requested {} while waiting for {} to complete.", fsm.getSessionName(),
                        event.getMetadata().getSyncId(), getTransitionSyncId());
                LogReplicationState snapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                snapshotSyncState.setTransitionSyncId(event.getMetadata().getSyncId());
                ((InSnapshotSyncState)snapshotSyncState).setForcedSnapshotSync(event.getMetadata().isForcedSnapshotSync());
                return snapshotSyncState;
            case SYNC_CANCEL:
                if(fsm.isValidTransition(transitionSyncId, event.getMetadata().getSyncId())) {
                    log.debug("[{}]:: Sync has been canceled while waiting for Snapshot Sync {} to complete apply. Restart.",
                            fsm.getSessionName(), transitionSyncId);
                    LogReplicationState inSnapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                    // new ID for new snapshot sync ID
                    inSnapshotSyncState.setTransitionSyncId(UUID.randomUUID());
                    ((InSnapshotSyncState) inSnapshotSyncState).setForcedSnapshotSync(event.getMetadata().isForcedSnapshotSync());
                    return inSnapshotSyncState;
                }
                log.info("[{}]:: Ignoring Sync cancel event for snapshot sync {}, as ongoing snapshot sync is {}", fsm.getSessionName(),
                        event.getMetadata().getSyncId(), transitionSyncId);
                return this;
            case SNAPSHOT_APPLY_IN_PROGRESS:
                if(fsm.isValidTransition(transitionSyncId, event.getMetadata().getSyncId())) {
                    log.debug("[{}]:: Snapshot Apply in progress {}. Verify status.", fsm.getSessionName(), transitionSyncId);
                    return this;
                }
                log.info("[{}]:: Ignoring Snapshot Apply in Progress event for snapshot sync {}, as ongoing snapshot sync is {}",
                        fsm.getSessionName(), event.getMetadata().getSyncId(), transitionSyncId);
                return this;
            case SNAPSHOT_APPLY_COMPLETE:
                /*
                 This is required as in the following sequence of events:

                 1. SNAPSHOT_SYNC_REQUEST (ID = 1) EXTERNAL
                 2. SYNC_CANCEL (ID = 1) EXTERNAL
                 3. SNAPSHOT_SYNC_REQUEST (ID = 2) EXTERNAL

                 Snapshot Sync with ID = 1 could be completed in between (1 and 2) but show up in the queue
                 as 4, attempting to process a completion event for the incorrect snapshot sync.
                 */

                if (fsm.isValidTransition(transitionSyncId, event.getMetadata().getSyncId())) {
                    LogReplicationState logEntrySyncState = fsm.getStates()
                            .get(LogReplicationStateType.IN_LOG_ENTRY_SYNC);
                    // We need to set a new transition event Id, so anything happening on this new state
                    // is marked with this unique Id and correlated to cancel or trimmed events.
                    logEntrySyncState.setTransitionSyncId(transitionSyncId);
                    fsm.setBaseSnapshot(event.getMetadata().getLastTransferredBaseSnapshot());
                    fsm.setAckedTimestamp(event.getMetadata().getLastLogEntrySyncedTimestamp());
                    log.info("[{}]:: Snapshot Sync apply completed, syncRequestId={}, baseSnapshot={}. Transition to LOG_ENTRY_SYNC",
                            fsm.getSessionName(), event.getMetadata().getSyncId(), event.getMetadata().getLastTransferredBaseSnapshot());
                    return logEntrySyncState;
                }

                log.warn("[{}]:: Ignoring snapshot sync apply complete event for snapshot sync {}, as ongoing snapshot sync is {}",
                        fsm.getSessionName(), event.getMetadata().getSyncId(), transitionSyncId);
                return this;
            case REPLICATION_STOP:
                // No need to validate transitionId as REPLICATION_STOP comes either from enforceSnapshotSync or when
                // the runtime FSM transitions back to VERIFYING_REMOTE_LEADER from REPLICATING state
                log.debug("[{}]:: Stop Log Replication while waiting for snapshot sync apply to complete id={}", fsm.getSessionName(), transitionSyncId);
                return fsm.getStates().get(LogReplicationStateType.INITIALIZED);
            case REPLICATION_SHUTDOWN:
                log.debug("[{}]:: Shutdown Log Replication while waiting for snapshot sync apply to complete id={}", fsm.getSessionName(), transitionSyncId);
                return fsm.getStates().get(LogReplicationStateType.ERROR);
            default: {
                if (!fsm.isValidTransition(transitionSyncId, event.getMetadata().getSyncId())) {
                    log.warn("[{}]:: Ignoring log replication event {} for sync {} when in wait snapshot sync apply state for sync {}",
                            fsm.getSessionName(), event.getType(), event.getMetadata().getSyncId(), transitionSyncId);
                    return this;
                }
                log.warn("[{}]:: Unexpected log replication event {} for sync {} when in wait snapshot sync apply state for sync {}",
                        fsm.getSessionName(), event.getType(), event.getMetadata().getSyncId(), transitionSyncId);
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(LogReplicationState from) {
        log.info("[{}]:: OnEntry :: wait snapshot apply state", fsm.getSessionName());
        if (from.getType().equals(LogReplicationStateType.INITIALIZED)) {
            fsm.getAckReader().setSyncType(SyncType.SNAPSHOT);
            fsm.getAckReader().markSnapshotSyncInfoOngoing();
        }
        //TODO V2: the metrics need to be per session
//        if (from != this) {
//            snapshotSyncApplyTimerSample = MeterRegistryProvider.getInstance().map(Timer::start);
//        }
        verifyStatusOfSnapshotSyncApply();
    }

    @Override
    public void onExit(LogReplicationState to) {
        // TODO V2: the metrics here need to be per session.

        //        if (to.getType().equals(LogReplicationStateType.IN_LOG_ENTRY_SYNC)) {
        //            snapshotSyncApplyTimerSample
        //                    .flatMap(sample -> MeterRegistryProvider.getInstance()
        //                            .map(registry -> {
        //                                Timer timer = registry.timer("logreplication.snapshot.apply.duration");
        //                                return sample.stop(timer);
        //                            }));
        //        }
    }

    private void verifyStatusOfSnapshotSyncApply() {
        try {
            log.info("[{}]:: Verify snapshot sync apply status, sync={}", fsm.getSessionName(), transitionSyncId);

            // Query metadata on remote cluster to verify the status of the snapshot sync apply
            CompletableFuture<LogReplicationMetadataResponseMsg>
                    metadataResponseCompletableFuture = dataSender.sendMetadataRequest();
            LogReplicationMetadataResponseMsg metadataResponse = metadataResponseCompletableFuture
                    .get(CorfuLogReplicationRuntime.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

            // If snapshot sync apply phase has been completed on remote cluster, transition to Log Entry Sync
            // (incremental update replication), otherwise, schedule new query.
            if (metadataResponse.getLastLogEntryTimestamp() == metadataResponse.getSnapshotApplied() &&
                    metadataResponse.getSnapshotApplied() == baseSnapshotTimestamp) {
                log.info("[{}]:: Snapshot sync apply is complete appliedTs={}, baseTs={}", fsm.getSessionName(),
                        metadataResponse.getSnapshotApplied(), baseSnapshotTimestamp);
                fsm.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_APPLY_COMPLETE,
                        new LogReplicationEventMetadata(transitionSyncId, baseSnapshotTimestamp, baseSnapshotTimestamp)));
                return;
            } else {
                log.debug("Snapshot sync apply is still in progress, appliedTs={}, baseTs={}, sync_id={}", metadataResponse.getSnapshotApplied(),
                        baseSnapshotTimestamp, transitionSyncId);
            }
        } catch (Exception e) {
            log.error("Snapshot sync apply verification failed.", e);
        }
        // Schedule a one time action which will verify the snapshot apply status after a given delay
        scheduleSnapshotApplyVerification();
    }

    private void scheduleSnapshotApplyVerification() {
        log.debug("[{}]:: Schedule verification of snapshot sync apply id={}", fsm.getSessionName(), transitionSyncId);
        fsm.inputWithDelay(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_APPLY_IN_PROGRESS,
                new LogReplicationEventMetadata(transitionSyncId)), SCHEDULE_APPLY_MONITOR_DELAY);
    }

    @Override
    public void setTransitionSyncId(UUID eventId) {
        this.transitionSyncId = eventId;
    }

    @Override
    public UUID getTransitionSyncId() { return transitionSyncId; }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.WAIT_SNAPSHOT_APPLY;
    }

    public void setBaseSnapshotTimestamp(long baseSnapshotTimestamp) {
        this.baseSnapshotTimestamp = baseSnapshotTimestamp;
    }
}
