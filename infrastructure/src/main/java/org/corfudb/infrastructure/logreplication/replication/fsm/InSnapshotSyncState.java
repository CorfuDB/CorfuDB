package org.corfudb.infrastructure.logreplication.replication.fsm;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal.SyncType;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SyncStatus;
import org.corfudb.infrastructure.logreplication.replication.send.SnapshotSender;

import java.util.Optional;
import java.util.UUID;
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
     * A SNAPSHOT_SYNC_REQUEST arriving less than this long after the current attempt's last genuine
     * (non self-loop) entry is treated as part of the same restart storm rather than a deliberate,
     * externally-triggered new request, and backs off instead of restarting unthrottled.
     */
    @VisibleForTesting
    static final long MIN_ATTEMPT_AGE_FOR_UNTHROTTLED_RESTART_MS = 3000;

    /**
     * Log Replication Finite State Machine Instance
     */
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
    private java.util.concurrent.ScheduledFuture<?> delayedTransmit;
    private volatile long requestedReset;
    private long appliedReset;
    private long retryNotBeforeNanos;
    private volatile long transmitGeneration;
    private static final java.util.concurrent.ScheduledExecutorService RETRIES =
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor(new com.google.common.util.concurrent.ThreadFactoryBuilder()
                    .setDaemon(true).setNameFormat("snapshot-retry-%d").build());

    /**
     * Indicates if the snapshot sync was forced by the caller (instead of determined by negotiation)
     */
    private boolean forcedSnapshotSync = false;

    /**
     * Number of consecutive cancellations/restarts of this snapshot sync, without a fresh externally
     * requested attempt or a full completion in between. Drives the exponential backoff.
     */
    @VisibleForTesting
    int consecutiveCancellations = 0;

    /**
     * Backoff to apply, in onEntry(), before retrying the next attempt after a cancellation. Zero
     * means no pending backoff (e.g. a plain SNAPSHOT_SYNC_CONTINUE self-loop).
     */
    @VisibleForTesting
    long retryBackoffMs = 0;

    /**
     * Wall-clock time of the last genuine (from != this) entry into this state, used to tell a fresh,
     * externally-requested SNAPSHOT_SYNC_REQUEST apart from one arriving mid-restart-storm.
     */
    @VisibleForTesting
    long lastEntryTimeMs = 0;

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

                // A request arriving shortly after the last genuine entry looks like part of the same
                // restart storm (e.g. a caller repeatedly invoking enforceSnapshotSync()) rather than a
                // deliberate new request, and should back off like a cancellation instead of restarting
                // unthrottled.
                boolean looksLikeRestartStorm =
                        (System.currentTimeMillis() - lastEntryTimeMs) < MIN_ATTEMPT_AGE_FOR_UNTHROTTLED_RESTART_MS;

                // Cancel ongoing snapshot sync, if it is still in progress.
                setForcedSnapshotSync(event.getMetadata().isForcedSnapshotSync());
                String cancelCause = forcedSnapshotSync ? "incoming forced snapshot sync." : "another snapshot sync request.";
                cancelSnapshotSync(cancelCause);

                if (looksLikeRestartStorm) {
                    registerCancellationAndComputeBackoff();
                } else {
                    resetBackoff();
                }
                lastEntryTimeMs = System.currentTimeMillis();

                // Set the id of the new snapshot sync request causing the transition.
                // This will be taken onEntry of this state to initiate a snapshot send for this given request.
                this.setTransitionSyncId(event.getMetadata().getSyncId());
                requestedReset++;
                fsm.getAckReader().markSnapshotSyncInfoOngoing(forcedSnapshotSync, transitionSyncId, consecutiveCancellations);
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
                    ((InSnapshotSyncState)inSnapshotSyncState).registerCancellationAndComputeBackoff();
                    ((InSnapshotSyncState)inSnapshotSyncState).lastEntryTimeMs = System.currentTimeMillis();
                    requestedReset++;
                    fsm.getAckReader().markSnapshotSyncInfoOngoing(forcedSnapshotSync, transitionSyncId,
                            ((InSnapshotSyncState) inSnapshotSyncState).consecutiveCancellations);
                    return inSnapshotSyncState;
                }

                log.warn("Ignoring Sync Cancel for eventId {}, while running snapshot sync for {}",
                        event.getMetadata().getSyncId(), transitionSyncId);
                return this;
            case REPLICATION_STOP:
                // No need to validate transitionId as REPLICATION_STOP comes either from enforceSnapshotSync or when
                // the runtime FSM transitions back to VERIFYING_REMOTE_LEADER from REPLICATING state
                cancelSnapshotSync("of a request to stop replication.");
                // A stop is a clean boundary; a later, unrelated session must not inherit this backoff.
                resetBackoff();
                return fsm.getStates().get(LogReplicationStateType.INITIALIZED);
            case REPLICATION_SHUTDOWN:
                cancelSnapshotSync("replication terminated.");
                resetBackoff();
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
            fsm.getAckReader().setSyncType(SyncType.SNAPSHOT);
            requestedReset++;
            fsm.getAckReader().markSnapshotSyncInfoOngoing(forcedSnapshotSync, transitionSyncId, consecutiveCancellations);
            snapshotSyncTransferTimerSample = MeterRegistryProvider.getInstance().map(Timer::start);
            lastEntryTimeMs = System.currentTimeMillis();
        }
        if (retryBackoffMs > 0) {
            retryNotBeforeNanos = Math.max(retryNotBeforeNanos,
                    System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(retryBackoffMs));
        }
        long delay = Math.max(0, retryNotBeforeNanos - System.nanoTime());
        retryBackoffMs = 0;
        long generation = ++transmitGeneration;
        long reset = requestedReset;
        UUID syncId = transitionSyncId;
        boolean forced = forcedSnapshotSync;
        Runnable dispatch = () -> {
            if (generation != transmitGeneration) { return; }
            transmitFuture = fsm.getLogReplicationFSMWorkers().submit(() -> {
                if (generation != transmitGeneration) { return; }
                // Reset on the same worker as transmit, after any cancelled old task returns.
                // A superseded queued continuation must not consume the pending reset.
                if (appliedReset != reset) {
                    snapshotSender.reset();
                    appliedReset = reset;
                }
                if (generation != transmitGeneration) { return; }
                snapshotSender.transmit(syncId, forced);
            });
        };
        if (delay > 0) {
            delayedTransmit = RETRIES.schedule(dispatch, delay, TimeUnit.NANOSECONDS);
        } else {
            dispatch.run();
        }
    }

    /**
     * Register another cancellation/restart of the current attempt and compute the next backoff:
     * INITIAL_RETRY_BACKOFF_MS on the first, doubling (capped at MAX_RETRY_BACKOFF_MS) on each
     * subsequent one.
     */
    void registerCancellationAndComputeBackoff() {
        registerCancellationAndComputeBackoff(0);
    }

    /**
     * As above, but the resulting backoff is floored at minBackoffMs -- deliberately *not* itself
     * capped at MAX_RETRY_BACKOFF_MS, unlike the normal exponential computation, since this floor
     * exists for a different purpose (letting the sink's checkpointer, just unfrozen because
     * isApplyRetriesExhausted() -- see LogReplicationSinkManager's Javadoc -- actually get a sized
     * window) than the general restart-storm throttling MAX_RETRY_BACKOFF_MS is tuned for, and the
     * two aren't necessarily meant to have the same ceiling. Zero (the default from every other
     * call site) is a no-op: the normal computed backoff always wins over a zero floor.
     */
    void registerCancellationAndComputeBackoff(long minBackoffMs) {
        if (snapshotSender.usesSnapshotLifecycle()) {
            retryBackoffMs = 0;
            retryNotBeforeNanos = 0;
            return;
        }
        consecutiveCancellations = Math.min(Integer.MAX_VALUE - 1, consecutiveCancellations) + 1;
        long computed = Math.min(LogReplicationConfig.INITIAL_RETRY_BACKOFF_MS
                * (1L << Math.min(30, consecutiveCancellations - 1)), LogReplicationConfig.MAX_RETRY_BACKOFF_MS);
        retryBackoffMs = Math.max(computed, minBackoffMs);
    }

    /**
     * Clear the backoff state, e.g. on a full completion, a genuinely fresh externally-requested
     * sync, or a clean stop/shutdown boundary.
     */
    void resetBackoff() {
        consecutiveCancellations = 0;
        retryBackoffMs = 0;
        retryNotBeforeNanos = 0;
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
        if (delayedTransmit != null) { delayedTransmit.cancel(false); }
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
