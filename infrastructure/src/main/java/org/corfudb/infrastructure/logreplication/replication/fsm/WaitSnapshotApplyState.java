package org.corfudb.infrastructure.logreplication.replication.fsm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationMetadataResponse;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
     Uniquely identifies the snapshot sync to which this wait state is associated.
     */
    private UUID transitionEventId;

    /**
     * Route query metadata messages to the remote cluster
     */
    private DataSender dataSender;

    /**
     * Base Snapshot Timestamp for current Snapshot Sync
     */
    private long baseSnapshotTimestamp;

    private ScheduledExecutorService snapshotSyncApplyMonitorExecutor;

    /**
     * Constructor
     *
     * @param logReplicationFSM log replication state machine
     */
    public WaitSnapshotApplyState(LogReplicationFSM logReplicationFSM, DataSender dataSender) {
        this.fsm = logReplicationFSM;
        this.dataSender = dataSender;
        this.snapshotSyncApplyMonitorExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("snapshotSyncApplyVerificationScheduler")
                .build());
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                log.info("Snapshot Sync requested {} while waiting for {} to complete.", event.getEventID(), getTransitionEventId());
                LogReplicationState snapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                snapshotSyncState.setTransitionEventId(event.getEventID());
                return snapshotSyncState;
            case SYNC_CANCEL:
                log.debug("Sync has been canceled while waiting for Snapshot Sync {} to complete apply. Restart.", transitionEventId);
                LogReplicationState inSnapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                inSnapshotSyncState.setTransitionEventId(UUID.randomUUID());
                return inSnapshotSyncState;
            case SNAPSHOT_APPLY_IN_PROGRESS:
                log.debug("Snapshot Apply in progress {}. Verify status.", transitionEventId);
                return this;
            case SNAPSHOT_APPLY_COMPLETE:
                UUID snapshotSyncApplyId = event.getMetadata().getRequestId();
                /*
                 This is required as in the following sequence of events:

                 1. SNAPSHOT_SYNC_REQUEST (ID = 1) EXTERNAL
                 2. SYNC_CANCEL (ID = 1) EXTERNAL
                 3. SNAPSHOT_SYNC_REQUEST (ID = 2) EXTERNAL

                 Snapshot Sync with ID = 1 could be completed in between (1 and 2) but show up in the queue
                 as 4, attempting to process a completion event for the incorrect snapshot sync.
                 */
                if (snapshotSyncApplyId.equals(transitionEventId)) {
                    LogReplicationState logEntrySyncState = fsm.getStates()
                            .get(LogReplicationStateType.IN_LOG_ENTRY_SYNC);
                    // We need to set a new transition event Id, so anything happening on this new state
                    // is marked with this unique Id and correlated to cancel or trimmed events.
                    logEntrySyncState.setTransitionEventId(event.getEventID());
                    fsm.setBaseSnapshot(event.getMetadata().getLastTransferredBaseSnapshot());
                    fsm.setAckedTimestamp(event.getMetadata().getLastLogEntrySyncedTimestamp());
                    log.info("Snapshot Sync apply completed, syncRequestId={}, baseSnapshot={}. Transition to LOG_ENTRY_SYNC",
                            event.getEventID(), event.getMetadata().getLastTransferredBaseSnapshot());
                    return logEntrySyncState;
                }

                log.warn("Ignoring snapshot sync apply complete event, for request {}, as ongoing snapshot sync is {}",
                        snapshotSyncApplyId, transitionEventId);
                return this;
            case REPLICATION_STOP:
                log.debug("Stop Log Replication while waiting for snapshot sync apply to complete id={}", transitionEventId);
                return fsm.getStates().get(LogReplicationStateType.INITIALIZED);
            case REPLICATION_SHUTDOWN:
                log.debug("Shutdown Log Replication while waiting for snapshot sync apply to complete id={}", transitionEventId);
                return fsm.getStates().get(LogReplicationStateType.STOPPED);
            default: {
                log.warn("Unexpected log replication event {} when in wait snapshot sync apply state.", event.getType());
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(LogReplicationState from) {
        log.info("OnEntry :: wait snapshot apply state");
        this.fsm.getLogReplicationFSMWorkers().submit(this::verifyStatusOfSnapshotSyncApply);
    }

    private void verifyStatusOfSnapshotSyncApply() {
        try {
            log.debug("Verify snapshot sync apply status, sync={}", transitionEventId);

            // Query metadata on remote cluster to verify the status of the snapshot sync apply
            CompletableFuture<LogReplicationMetadataResponse> metadataResponseCompletableFuture = dataSender.sendMetadataRequest();
            LogReplicationMetadataResponse metadataResponse = metadataResponseCompletableFuture.get(CorfuLogReplicationRuntime.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

            // If snapshot sync apply phase has been completed on remote cluster, transition to Log Entry Sync
            // (incremental update replication), otherwise, schedule new query.
            if (metadataResponse.getLastLogProcessed() == metadataResponse.getSnapshotApplied() &&
                    metadataResponse.getSnapshotApplied() == baseSnapshotTimestamp) {
                log.info("Snapshot sync apply is complete appliedTs={}, baseTs={}", metadataResponse.getSnapshotApplied(),
                        baseSnapshotTimestamp);
                fsm.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_APPLY_COMPLETE,
                        new LogReplicationEventMetadata(transitionEventId, baseSnapshotTimestamp, baseSnapshotTimestamp)));
            } else {
                log.debug("Snapshot sync apply is still in progress, appliedTs={}, baseTs={}, sync_id={}", metadataResponse.getSnapshotApplied(),
                        baseSnapshotTimestamp, transitionEventId);
                // Schedule a one time action which will verify the snapshot apply status after a given delay
                this.snapshotSyncApplyMonitorExecutor.schedule(this::scheduleSnapshotApplyVerification, SCHEDULE_APPLY_MONITOR_DELAY,
                        TimeUnit.MILLISECONDS);
            }
        } catch (TimeoutException te) {
            log.error("Snapshot sync apply verification timed out.", te);
            // Schedule a one time action which will verify the snapshot apply status after a given delay
            this.snapshotSyncApplyMonitorExecutor.schedule(this::scheduleSnapshotApplyVerification, SCHEDULE_APPLY_MONITOR_DELAY,
                    TimeUnit.MILLISECONDS);
        } catch (ExecutionException ee) {
            // Completable future completed exceptionally
            log.error("Snapshot sync apply verification failed.", ee);
            // Schedule a one time action which will verify the snapshot apply status after a given delay
            this.snapshotSyncApplyMonitorExecutor.schedule(this::scheduleSnapshotApplyVerification, SCHEDULE_APPLY_MONITOR_DELAY,
                    TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("Snapshot sync apply verification failed.", e);
            // Schedule a one time action which will verify the snapshot apply status after a given delay
            this.snapshotSyncApplyMonitorExecutor.schedule(this::scheduleSnapshotApplyVerification, SCHEDULE_APPLY_MONITOR_DELAY,
                    TimeUnit.MILLISECONDS);
        }
    }

    private void scheduleSnapshotApplyVerification() {
        log.debug("Schedule verification of snapshot sync apply id={}", transitionEventId);
        fsm.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_APPLY_IN_PROGRESS,
                new LogReplicationEventMetadata(transitionEventId)));
    }

    @Override
    public void setTransitionEventId(UUID eventId) {
        this.transitionEventId = eventId;
    }

    @Override
    public UUID getTransitionEventId() { return transitionEventId; }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.WAIT_SNAPSHOT_APPLY;
    }

    public void setBaseSnapshotTimestamp(long baseSnapshotTimestamp) {
        this.baseSnapshotTimestamp = baseSnapshotTimestamp;
    }
}
