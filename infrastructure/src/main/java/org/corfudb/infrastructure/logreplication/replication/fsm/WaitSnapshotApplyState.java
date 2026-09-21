package org.corfudb.infrastructure.logreplication.replication.fsm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.Timer;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal.SyncType;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.infrastructure.logreplication.replication.send.SnapshotSender;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Phase;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class represents the WaitSnapshotApply state of the Log Replication State Machine.
 *
 * In this state the remote cluster is queried to verify snapshot sync has been applied
 * and move onto log entry sync (incremental update replication).
 *
 * This state is an optimization such that snapshot sync is separated into transfer and apply phases.
 * If data has been completely transferred and some failure occurs immediately after, the receiver can still
 * recover and data does not need to be transferred all over again.
 *
 * The source has no timer of its own here. It follows the sink's snapshot lease: the attempt either
 * completes, or the sink abandons it (its deadline, a failed apply, a change of owner or topology)
 * and reports that, at which point the source cancels and asks for a new attempt.
 */
@Slf4j
public class WaitSnapshotApplyState implements LogReplicationState {

    /**
     * Delay in milliseconds to monitor replication status on receiver, when snapshot sync apply is in progress.
     */
    private static final int SCHEDULE_APPLY_MONITOR_DELAY = 2000;

    /**
     * The first checks come sooner, each twice as late as the one before, up to the delay above. The
     * apply of a small snapshot takes a fraction of a second: asked only every two seconds, every
     * such snapshot sync would take two seconds longer than it does.
     */
    private static final int FIRST_APPLY_MONITOR_DELAY = 250;

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
     * Used for checking LR is in upgrading path or not
     */
    private final LogReplicationConfigManager tableManagerPlugin;

    /**
     * Base Snapshot Timestamp for current Snapshot Sync
     */
    private long baseSnapshotTimestamp;

    private final ScheduledExecutorService snapshotSyncApplyMonitorExecutor;
    private volatile long verificationGeneration;
    // Checks scheduled since this state was entered, which paces the next one. Only touched by the
    // FSM worker, like the verification itself.
    private int scheduledVerifications;
    private java.util.concurrent.ScheduledFuture<?> pendingVerification;

    private Optional<Timer.Sample> snapshotSyncApplyTimerSample = Optional.empty();

    @Setter
    private boolean forcedSnapshotSync;

    private boolean unsupportedSinkReported;

    /**
     * Constructor
     *
     * @param logReplicationFSM log replication state machine
     */
    public WaitSnapshotApplyState(LogReplicationFSM logReplicationFSM, DataSender dataSender, LogReplicationConfigManager tableManagerPlugin) {
        this.fsm = logReplicationFSM;
        this.dataSender = dataSender;
        this.snapshotSyncApplyMonitorExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("snapshotSyncApplyVerificationScheduler")
                .build());
        this.tableManagerPlugin = tableManagerPlugin;
        this.forcedSnapshotSync = false;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) throws IllegalTransitionException {
        InSnapshotSyncState snapshotState = (InSnapshotSyncState) fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
        if (snapshotState != null && !event.getMetadata().matchesSnapshotAttempt(snapshotState.getSnapshotSender())) {
            return this;
        }
        switch (event.getType()) {
            case SNAPSHOT_SYNC_CONTINUE:
                // A transfer continuation already queued when transfer completed is harmless.
                return this;
            case SNAPSHOT_SYNC_REQUEST:
                log.info("Snapshot Sync requested {} while waiting for {} to complete.",
                        event.getMetadata().getSyncId(), getTransitionSyncId());
                LogReplicationState snapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                snapshotSyncState.setTransitionSyncId(event.getMetadata().getSyncId());
                ((InSnapshotSyncState)snapshotSyncState).setForcedSnapshotSync(event.getMetadata().isForcedSnapshotSync());
                // A deliberate new request starts a new run of attempts. It cannot cause a restart
                // storm on the sink: the new attempt is only admitted once the sink allows it.
                ((InSnapshotSyncState) snapshotSyncState).resetCancellations();
                return snapshotSyncState;
            case SYNC_CANCEL:
                if(fsm.isValidTransition(transitionSyncId, event.getMetadata().getSyncId())) {
                    log.debug("Sync has been canceled while waiting for Snapshot Sync {} to complete apply. Restart.", transitionSyncId);
                    LogReplicationState inSnapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                    // If the cancelled sync was a force sync, retain the syncID, else generate a new sync ID
                    UUID newSnapshotSyncId = event.getMetadata().isForcedSnapshotSync() ? event.getMetadata().getSyncId() : UUID.randomUUID();
                    inSnapshotSyncState.setTransitionSyncId(newSnapshotSyncId);
                    ((InSnapshotSyncState) inSnapshotSyncState).setForcedSnapshotSync(event.getMetadata().isForcedSnapshotSync());
                    ((InSnapshotSyncState) inSnapshotSyncState).registerCancellation();
                    return inSnapshotSyncState;
                }
                log.info("Ignoring Sync cancel event for snapshot sync {}, as ongoing snapshot sync is {}",
                        event.getMetadata().getSyncId(), transitionSyncId);
                return this;
            case SNAPSHOT_APPLY_IN_PROGRESS:
                if(fsm.isValidTransition(transitionSyncId, event.getMetadata().getSyncId())) {
                    log.debug("Snapshot Apply in progress {}. Verify status.", transitionSyncId);
                    return this;
                }
                log.info("Ignoring Snapshot Apply in Progress event for snapshot sync {}, as ongoing snapshot sync is {}",
                        event.getMetadata().getSyncId(), transitionSyncId);
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
                    if (tableManagerPlugin.isUpgraded()) {
                        // If LR is in upgrading path, it means this cycle of snapshot sync was triggered
                        // forcibly because LR detected a version mismatch. Flipping the flag back to false
                        // here to indicate that the upgrade path is completed.
                        log.info("Forced snapshot sync due to LR upgrade is COMPLETE.");
                        tableManagerPlugin.resetUpgradeFlag();
                    }
                    // remove the force snapshot request from the event table
                    if (forcedSnapshotSync) {
                        fsm.getAckReader().getMetadataManager().clearEventTable();
                        log.info("Finished processing event {}. Flushing all events from the event table", transitionSyncId);
                    }
                    log.info("Snapshot Sync apply completed, syncRequestId={}, baseSnapshot={}. Transition to LOG_ENTRY_SYNC",
                            event.getMetadata().getSyncId(), event.getMetadata().getLastTransferredBaseSnapshot());
                    // Full end-to-end completion ends the current run of attempts.
                    ((InSnapshotSyncState) fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC)).resetCancellations();
                    return logEntrySyncState;
                }

                log.warn("Ignoring snapshot sync apply complete event for snapshot sync {}, as ongoing snapshot sync is {}",
                        event.getMetadata().getSyncId(), transitionSyncId);
                return this;
            case REPLICATION_STOP:
                // No need to validate transitionId as REPLICATION_STOP comes either from enforceSnapshotSync or when
                // the runtime FSM transitions back to VERIFYING_REMOTE_LEADER from REPLICATING state
                log.debug("Stop Log Replication while waiting for snapshot sync apply to complete id={}", transitionSyncId);
                // A stop is a clean boundary; a later, unrelated session must not inherit this count.
                ((InSnapshotSyncState) fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC)).resetCancellations();
                return fsm.getStates().get(LogReplicationStateType.INITIALIZED);
            case REPLICATION_SHUTDOWN:
                log.debug("Shutdown Log Replication while waiting for snapshot sync apply to complete id={}", transitionSyncId);
                ((InSnapshotSyncState) fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC)).resetCancellations();
                return fsm.getStates().get(LogReplicationStateType.ERROR);
            default: {
                if (!fsm.isValidTransition(transitionSyncId, event.getMetadata().getSyncId())) {
                    log.warn("Ignoring log replication event {} for sync {} when in wait snapshot sync apply state for sync {}",
                            event.getType(), event.getMetadata().getSyncId(), transitionSyncId);
                    return this;
                }
                log.warn("Unexpected log replication event {} for sync {} when in wait snapshot sync apply state for sync {}",
                        event.getType(), event.getMetadata().getSyncId(), transitionSyncId);
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(LogReplicationState from) {
        log.info("OnEntry :: wait snapshot apply state");
        if (from.getType().equals(LogReplicationStateType.INITIALIZED)) {
            fsm.getAckReader().setSyncType(SyncType.SNAPSHOT);
            fsm.getAckReader().markSnapshotSyncInfoOngoing();
        }
        if (from != this) {
            verificationGeneration++;
            scheduledVerifications = 0;
            snapshotSyncApplyTimerSample = MeterRegistryProvider.getInstance().map(Timer::start);
        }
        long generation = verificationGeneration;
        this.fsm.getLogReplicationFSMWorkers().submit(() -> {
            if (generation == verificationGeneration) { verifyStatusOfSnapshotSyncApply(); }
        });
    }

    @Override
    public void onExit(LogReplicationState to) {
        if (to != this) {
            verificationGeneration++;
            if (pendingVerification != null) { pendingVerification.cancel(false); }
        }
        if (to.getType().equals(LogReplicationStateType.IN_LOG_ENTRY_SYNC)) {
            snapshotSyncApplyTimerSample
                    .flatMap(sample -> MeterRegistryProvider.getInstance()
                            .map(registry -> {
                                Timer timer = registry.timer("logreplication.snapshot.apply.duration");
                                return sample.stop(timer);
                            }));
        }
    }

    @VisibleForTesting
    void verifyStatusOfSnapshotSyncApply() {
        long generation = verificationGeneration;
        UUID verifyingId = transitionSyncId;
        try {
            log.info("Verify snapshot sync apply status, sync={}", transitionSyncId);

            // Query metadata on remote cluster to verify the status of the snapshot sync apply
            CompletableFuture<LogReplicationMetadataResponseMsg>
                    metadataResponseCompletableFuture = dataSender.sendMetadataRequest();
            LogReplicationMetadataResponseMsg metadataResponse = metadataResponseCompletableFuture
                    .get(CorfuLogReplicationRuntime.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
            if (generation != verificationGeneration) { return; }
            if (!metadataResponse.hasSnapshotLease()) {
                // Negotiation refuses a sink without the snapshot lease, so this is only reachable
                // when replication is driven without it. Nothing can be concluded: keep asking.
                if (!unsupportedSinkReported) {
                    log.error("The sink does not report a snapshot lease; cannot follow the apply of {}", transitionSyncId);
                    unsupportedSinkReported = true;
                }
                scheduleVerification(generation, verifyingId);
                return;
            }
            unsupportedSinkReported = false;
            SnapshotSyncLeaseRecord lease = metadataResponse.getSnapshotLease();
            InSnapshotSyncState snapshotState = (InSnapshotSyncState) fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
            SnapshotSender source = snapshotState.getSnapshotSender();
            boolean matching = source.getWireAttemptId() != null
                    && lease.getAttemptId().equals(CorfuProtocolCommon.getUuidMsg(source.getWireAttemptId()))
                    && lease.getGeneration() == source.getWireAttemptGeneration()
                    && lease.getTopologyConfigId() == fsm.getTopologyConfigId()
                    && lease.getSourceSnapshot() == baseSnapshotTimestamp;
            if (matching && lease.getOutcome() == Outcome.COMPLETED) {
                // Completion survives the sink's cleanup and recovery phases: the outcome stays
                // COMPLETED until the next attempt is admitted.
                log.info("Snapshot sync apply is complete, generation={}, baseTs={}", lease.getGeneration(), baseSnapshotTimestamp);
                fsm.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_APPLY_COMPLETE,
                        new LogReplicationEventMetadata(verifyingId, baseSnapshotTimestamp, baseSnapshotTimestamp, forcedSnapshotSync)
                                .setSnapshotAttempt(source.getWireAttemptId(), source.getWireAttemptGeneration())));
                return;
            }
            if (lease.getPhase() != Phase.NOT_READY && (!matching || lease.getOutcome() == Outcome.ABORTED)) {
                // The sink abandoned this attempt, or has moved on to another one. It will not
                // complete: cancel and ask for a new attempt, which the sink admits when it is ready.
                log.warn("The sink no longer runs snapshot sync {}: phase={}, outcome={}, failure={}. Restarting.",
                        transitionSyncId, lease.getPhase(), lease.getOutcome(), lease.getFailure());
                fsm.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL,
                        new LogReplicationEventMetadata(verifyingId, forcedSnapshotSync)
                                .setSnapshotAttempt(source.getWireAttemptId(), source.getWireAttemptGeneration())));
                return;
            }
            log.debug("Snapshot sync apply is still in progress, phase={}, baseTs={}, sync_id={}", lease.getPhase(),
                    baseSnapshotTimestamp, transitionSyncId);
        } catch (Exception e) {
            // A lost or late reply says nothing about the apply. The sink's own deadline bounds it.
            log.error("Snapshot sync apply verification failed.", e);
        }

        // Schedule a one time action which will verify the snapshot apply status after a given delay
        scheduleVerification(generation, verifyingId);
    }

    private void scheduleVerification(long generation, UUID verifyingId) {
        pendingVerification = snapshotSyncApplyMonitorExecutor.schedule(() -> {
            if (generation == verificationGeneration) {
                fsm.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_APPLY_IN_PROGRESS,
                        new LogReplicationEventMetadata(verifyingId)));
            }
        }, nextVerificationDelayMs(), TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    long nextVerificationDelayMs() {
        // 250, 500, 1000, then every 2000.
        int doublings = Math.min(scheduledVerifications++, 3);
        return Math.min(SCHEDULE_APPLY_MONITOR_DELAY, (long) FIRST_APPLY_MONITOR_DELAY << doublings);
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
