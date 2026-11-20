package org.corfudb.infrastructure.logreplication.replication.fsm;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal.SyncType;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SyncStatus;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.infrastructure.logreplication.replication.send.SnapshotSender;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class represents the InSnapshotSync state of the Log Replication State Machine.
 *
 * In this state full logs are being synced to the remote cluster, based on a snapshot timestamp.
 */
@Slf4j
public class InSnapshotSyncState implements LogReplicationState {

    /**
     * Log Replication Finite State Machine Instance
     */
    // Pause before the reset of the snapshot sender is tried again after it failed.
    private static final long RESET_RETRY_DELAY_MS = 1000;

    private final LogReplicationFSM fsm;

    private final Optional<AtomicLong> snapshotSyncAcksCounter;

    private Optional<Timer.Sample> snapshotSyncTransferTimerSample = Optional.empty();

    /**
     * Uniquely identifies the sync that caused the transition to this state.
     * This is required in the case that a snapshot sync is canceled and another snapshot sync is requested,
     * so the application can discard messages received for the previous snapshot sync, until the new
     * request (event) is handled.
     */
    private UUID transitionSyncId;

    /**
     * Read and send a snapshot of the data-store.
     */
    @Getter
    @VisibleForTesting
    private final SnapshotSender snapshotSender;

    /**
     * A future on the send, in case we need to cancel the ongoing snapshot sync.
     */
    private Future<?> transmitFuture;

    // A restart is requested by the FSM thread and applied by the FSM worker right before the next
    // transmit, behind whatever the cancelled task was still doing. A queued continuation that was
    // superseded in the meantime must not consume the pending reset.
    private volatile long requestedReset;
    private long appliedReset;
    private volatile long transmitGeneration;

    /**
     * Indicates if the snapshot sync was forced by the caller (instead of determined by negotiation)
     */
    private boolean forcedSnapshotSync = false;

    /**
     * Number of consecutive cancellations/restarts of this snapshot sync, without a fresh externally
     * requested attempt or a full completion in between. It is only reported (see SnapshotSyncInfo):
     * the source does not pace its restarts, because the sink does. A restarted attempt is not
     * admitted until the sink has cleaned up the previous one and its checkpointer has caught up.
     */
    @VisibleForTesting
    int consecutiveCancellations = 0;

    /**
     * Constructor
     *
     * @param logReplicationFSM log replication state machine
     * @param snapshotSender    snapshot sync send (read and send)
     */
    public InSnapshotSyncState(LogReplicationFSM logReplicationFSM, SnapshotSender snapshotSender) {
        this.fsm = logReplicationFSM;
        this.snapshotSender = snapshotSender;
        this.snapshotSyncAcksCounter = configureSnapshotSyncCounter();
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) throws IllegalTransitionException {
        if (!event.getMetadata().matchesSnapshotAttempt(snapshotSender)
                || (event.getMetadata().getSnapshotAttemptId() != null && requestedReset != appliedReset)) {
            return this;
        }
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                // Cancel ongoing snapshot sync, if it is still in progress.
                setForcedSnapshotSync(event.getMetadata().isForcedSnapshotSync());
                String cancelCause = forcedSnapshotSync ? "incoming forced snapshot sync." : "another snapshot sync request.";
                cancelSnapshotSync(cancelCause);
                // A deliberate new request starts a new run of attempts.
                resetCancellations();

                // Set the id of the new snapshot sync request causing the transition.
                // This will be taken onEntry of this state to initiate a snapshot send for this given request.
                this.setTransitionSyncId(event.getMetadata().getSyncId());
                requestedReset++;
                publishOngoing(false);
                return this;
            case SNAPSHOT_SYNC_CONTINUE:
                /*
                 Snapshot sync is broken into multiple tasks, where each task sends a batch of messages
                 corresponding to this snapshot sync. This is done to accommodate the case
                 of multi-cluster replication sharing a common thread pool, continuation allows to send another
                 batch of updates for the current snapshot sync.
                 */
                if (fsm.isValidTransition(transitionSyncId, event.getMetadata().getSyncId())) {
                    log.debug("InSnapshotSync[{}] :: Continuation of snapshot sync", transitionSyncId);
                } else {
                    log.warn("Ignoring snapshot sync continue for snapshot_sync ID {} when in snapshot_sync ID {}.",
                            event.getMetadata().getSyncId(), transitionSyncId);
                }
                return this;
            case SNAPSHOT_TRANSFER_COMPLETE:
                if (fsm.isValidTransition(transitionSyncId, event.getMetadata().getSyncId())) {
                    log.info("Snapshot Sync transfer is complete for {}", event.getMetadata().getSyncId());
                    WaitSnapshotApplyState waitSnapshotApplyState = (WaitSnapshotApplyState) fsm.getStates()
                            .get(LogReplicationStateType.WAIT_SNAPSHOT_APPLY);
                    waitSnapshotApplyState.setTransitionSyncId(transitionSyncId);
                    waitSnapshotApplyState.setBaseSnapshotTimestamp(snapshotSender.getBaseSnapshotTimestamp());
                    waitSnapshotApplyState.setForcedSnapshotSync(event.getMetadata().isForcedSnapshotSync());
                    fsm.setBaseSnapshot(event.getMetadata().getLastTransferredBaseSnapshot());
                    fsm.setAckedTimestamp(event.getMetadata().getLastLogEntrySyncedTimestamp());
                    snapshotSyncAcksCounter.ifPresent(AtomicLong::getAndIncrement);
                    return waitSnapshotApplyState;
                }
                log.warn("Ignoring Sync Transfer Complete for eventId {}, while running snapshot sync for {}",
                        event.getMetadata().getSyncId(), transitionSyncId);
                return this;
            case SYNC_CANCEL:
                // If cancel was intended for current snapshot sync task, cancel and transition to new state
                if (fsm.isValidTransition(transitionSyncId, event.getMetadata().getSyncId())) {
                    cancelSnapshotSync("cancellation request.");
                    LogReplicationState inSnapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                    // If the cancelled sync is a force snapshot sync, retain the syncID. This is to track and clear
                    // the snapshot sync requests in the eventTable
                    UUID newSnapshotSyncId = event.getMetadata().isForcedSnapshotSync() ? event.getMetadata().getSyncId() : UUID.randomUUID();
                    log.debug("Starting new snapshot sync after cancellation. forced {} ID={}", event.getMetadata().isForcedSnapshotSync(), newSnapshotSyncId);
                    inSnapshotSyncState.setTransitionSyncId(newSnapshotSyncId);
                    // If a force snapshot sync gets cancelled due to ACK timeout, a new snapshot sync is triggered.
                    // Retain the 'forced' information in the subsequent snapshot syncs
                    ((InSnapshotSyncState)inSnapshotSyncState).setForcedSnapshotSync(event.getMetadata().isForcedSnapshotSync());
                    ((InSnapshotSyncState)inSnapshotSyncState).registerCancellation();
                    requestedReset++;
                    // inSnapshotSyncState is this state: the count registered above is published.
                    publishOngoing(false);
                    return inSnapshotSyncState;
                }

                log.warn("Ignoring Sync Cancel for eventId {}, while running snapshot sync for {}",
                        event.getMetadata().getSyncId(), transitionSyncId);
                return this;
            case REPLICATION_STOP:
                // No need to validate transitionId as REPLICATION_STOP comes either from enforceSnapshotSync or when
                // the runtime FSM transitions back to VERIFYING_REMOTE_LEADER from REPLICATING state
                cancelSnapshotSync("of a request to stop replication.");
                // A stop is a clean boundary; a later, unrelated session must not inherit this count.
                resetCancellations();
                return fsm.getStates().get(LogReplicationStateType.INITIALIZED);
            case REPLICATION_SHUTDOWN:
                cancelSnapshotSync("replication terminated.");
                resetCancellations();
                return fsm.getStates().get(LogReplicationStateType.ERROR);
            default: {
                if (!fsm.isValidTransition(transitionSyncId, event.getMetadata().getSyncId())) {
                    log.warn("Ignoring log replication event {} for sync {} when in snapshot sync state for sync {}",
                            event.getType(), event.getMetadata().getSyncId(), transitionSyncId);
                    return this;
                }
                log.warn("Unexpected log replication event {} for sync {} when in snapshot sync state for sync {}.",
                        event.getType(), event.getMetadata().getSyncId(), transitionSyncId);
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(LogReplicationState from) {
        if (from != this) {
            // The restart must be requested whatever happens to the status update below.
            requestedReset++;
            publishOngoing(true);
            snapshotSyncTransferTimerSample = MeterRegistryProvider.getInstance().map(Timer::start);
        }
        long generation = ++transmitGeneration;
        long reset = requestedReset;
        UUID syncId = transitionSyncId;
        boolean forced = forcedSnapshotSync;
        try {
            transmitFuture = fsm.getLogReplicationFSMWorkers().submit(() -> {
                if (generation != transmitGeneration) { return; }
                try {
                    // Reset on the same worker as transmit, after any cancelled old task returns.
                    // A superseded queued continuation must not consume the pending reset.
                    if (appliedReset != reset) {
                        snapshotSender.reset();
                        appliedReset = reset;
                    }
                } catch (RuntimeException e) {
                    // The worker only records a failure in a Future nobody reads. Without another
                    // entry the state would sit here silently, so ask for one after a pause.
                    log.error("Could not reset the snapshot sender for {}; retrying", syncId, e);
                    CompletableFuture.runAsync(() -> fsm.input(new LogReplicationEvent(
                                    LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_CONTINUE,
                                    new LogReplicationEventMetadata(syncId))),
                            CompletableFuture.delayedExecutor(RESET_RETRY_DELAY_MS, TimeUnit.MILLISECONDS));
                    return;
                }
                if (generation != transmitGeneration) { return; }
                snapshotSender.transmit(syncId, forced);
            });
        } catch (Throwable t) {
            log.error("Error on entry of InSnapshotSyncState.", t);
        }
    }

    /**
     * Publishes the status of this run of attempts. It is informational, and it runs on the FSM's
     * consumer thread, which does not survive an exception: a failure to publish is logged, never
     * propagated.
     */
    private void publishOngoing(boolean enteringSnapshotSync) {
        try {
            if (enteringSnapshotSync) {
                fsm.getAckReader().setSyncType(SyncType.SNAPSHOT);
            }
            fsm.getAckReader().markSnapshotSyncInfoOngoing(forcedSnapshotSync, transitionSyncId, consecutiveCancellations);
        } catch (Throwable e) {
            // Errors included: the status update gives up with one when its retries are interrupted.
            log.error("Could not publish the status of snapshot sync {}", transitionSyncId, e);
        }
    }

    /** Another cancellation/restart of the current run of attempts. */
    void registerCancellation() {
        consecutiveCancellations = Math.min(Integer.MAX_VALUE - 1, consecutiveCancellations) + 1;
    }

    /**
     * A full completion, a genuinely fresh externally-requested sync, or a clean stop/shutdown
     * boundary ends the current run of attempts.
     */
    void resetCancellations() {
        consecutiveCancellations = 0;
    }

    @Override
    public void onExit(LogReplicationState to) {
        if (to.getType().equals(LogReplicationStateType.WAIT_SNAPSHOT_APPLY)) {
            snapshotSyncTransferTimerSample
                    .flatMap(sample -> MeterRegistryProvider.getInstance()
                            .map(registry -> {
                                Timer timer = registry.timer("logreplication.snapshot.transfer.duration");
                                return sample.stop(timer);
                            }));
        }
        if (to.getType().equals(LogReplicationStateType.INITIALIZED)) {
            fsm.getAckReader().markSyncStatus(SyncStatus.STOPPED);
            log.debug("Snapshot sync status changed to STOPPED");
        }
    }

    public void setTransitionSyncId(UUID eventId) {
        this.transitionSyncId = eventId;
    }

    public UUID getTransitionSyncId() { return transitionSyncId; }

    /**
     * Force interruption of the ongoing snapshot sync task.
     *
     * @param cancelCause cancel cause description
     */
    private void cancelSnapshotSync(String cancelCause) {
        transmitGeneration++;
        snapshotSender.stop();
        if (transmitFuture != null) { transmitFuture.cancel(true); }
        // A cancelled Future is not proof of termination. The single worker serializes
        // the next reset behind old work; the FSM itself does not block waiting for it.
        log.info("Snapshot sync is ending because {}", cancelCause);
    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.IN_SNAPSHOT_SYNC;
    }

    private Optional<AtomicLong> configureSnapshotSyncCounter() {
        return MeterRegistryProvider.getInstance()
                .map(registry -> registry.gauge("logreplication.snapshot.completed.count",
                        new AtomicLong(0)));
    }

    public void setForcedSnapshotSync(boolean forced) {
        forcedSnapshotSync = forced;
    }
}
