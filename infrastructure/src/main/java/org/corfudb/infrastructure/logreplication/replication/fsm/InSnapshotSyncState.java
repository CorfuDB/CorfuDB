package org.corfudb.infrastructure.logreplication.replication.fsm;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
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

    /**
     * Indicates if the snapshot sync was forced by the caller (instead of determined by negotiation)
     */
    private boolean forcedSnapshotSync = false;

    /**
     * Number of consecutive times this snapshot sync has been canceled and immediately retried
     * (e.g. due to repeated ack timeouts). Used to compute an increasing backoff so a struggling
     * sink is not hammered with a fresh full snapshot sync attempt every few seconds, which leaves
     * it no window to ever catch up (e.g. to complete local checkpointing that was paused for the
     * duration of the snapshot sync).
     * Package-private (not private) so tests can observe backoff growth directly.
     */
    @VisibleForTesting
    int consecutiveCancellations = 0;

    /**
     * Backoff (in milliseconds) to wait before the next transmit() call, set when a cancellation
     * triggers a retry. Zero means proceed immediately (e.g. a fresh, externally requested sync).
     * Package-private (not private) so tests can inspect/seed it directly.
     */
    @VisibleForTesting
    volatile long retryBackoffMs = 0;

    @VisibleForTesting
    static final long INITIAL_RETRY_BACKOFF_MS = 2_000;
    @VisibleForTesting
    static final long MAX_RETRY_BACKOFF_MS = 60_000;
    private static final int MAX_BACKOFF_SHIFT = 6; // caps 2^6 * INITIAL_RETRY_BACKOFF_MS before MAX applies anyway

    /**
     * Wall-clock time (epoch millis) this state was last entered (including a self-loop re-entry
     * after a cancellation). Used only to detect a SNAPSHOT_SYNC_REQUEST arriving suspiciously
     * soon after entry -- see the SNAPSHOT_SYNC_REQUEST handling below.
     * Package-private (not private) so tests can seed/inspect it directly.
     */
    @VisibleForTesting
    volatile long lastEntryTimeMs = 0;

    /**
     * SNAPSHOT_SYNC_REQUEST does not flow through SYNC_CANCEL, so it normally resets the backoff
     * unconditionally below (correct for a genuinely fresh, externally requested sync). But this
     * state can also receive repeated SNAPSHOT_SYNC_REQUESTs while already active -- e.g. a caller
     * invoking enforceSnapshotSync() repeatedly, or overlapping negotiation retriggers -- with no
     * throttling of its own. If a request arrives less than this long after we last entered the
     * state, it is treated as part of the same restart storm instead of resetting the backoff to
     * zero. Note this does not cover a snapshot sync restarted after an actual disconnect/
     * reconnect cycle, which re-enters via InitializedState rather than this state; throttling
     * that path was judged out of scope for this fix.
     */
    @VisibleForTesting
    static final long MIN_ATTEMPT_AGE_FOR_UNTHROTTLED_RESTART_MS = INITIAL_RETRY_BACKOFF_MS;

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
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:

                // Cancel ongoing snapshot sync, if it is still in progress.
                setForcedSnapshotSync(event.getMetadata().isForcedSnapshotSync());
                String cancelCause = forcedSnapshotSync ? "incoming forced snapshot sync." : "another snapshot sync request.";
                cancelSnapshotSync(cancelCause);

                long timeSinceLastEntry = System.currentTimeMillis() - lastEntryTimeMs;
                if (lastEntryTimeMs == 0 || timeSinceLastEntry >= MIN_ATTEMPT_AGE_FOR_UNTHROTTLED_RESTART_MS) {
                    // This is a freshly, externally requested snapshot sync (e.g. negotiation/reconnection),
                    // not an internal retry after a transient failure, so it should not inherit any pending backoff.
                    consecutiveCancellations = 0;
                    retryBackoffMs = 0;
                } else {
                    // Arrived too soon after the current attempt started to be a distinct, deliberate
                    // request; treat it the same as a SYNC_CANCEL-driven retry so a caller repeatedly
                    // requesting a snapshot sync (e.g. enforceSnapshotSync()) cannot restart unthrottled.
                    long backoff = registerCancellationAndComputeBackoff();
                    log.warn("Snapshot sync requested only {}ms after the current attempt started, remote={}; " +
                                    "treating as part of the same restart storm and backing off {}ms before retrying with ID={}",
                            timeSinceLastEntry, fsm.getAckReader().getRemoteClusterId(), backoff, event.getMetadata().getSyncId());
                }

                // Set the id of the new snapshot sync request causing the transition.
                // This will be taken onEntry of this state to initiate a snapshot send for this given request.
                this.setTransitionSyncId(event.getMetadata().getSyncId());
                snapshotSender.reset();
                fsm.getAckReader().markSnapshotSyncInfoOngoing(forcedSnapshotSync, transitionSyncId);
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

                    // Back off before retrying: a cancel-and-immediate-retry loop gives a struggling
                    // sink no window to catch up (e.g. to resume local checkpointing that is paused for
                    // the duration of the snapshot sync), so increase the delay with each consecutive
                    // cancellation instead of restarting at a fixed, rapid cadence.
                    long backoff = registerCancellationAndComputeBackoff();
                    log.warn("Snapshot sync canceled {} consecutive time(s), remote={}; backing off {}ms before retrying with ID={}",
                            consecutiveCancellations, fsm.getAckReader().getRemoteClusterId(), backoff, newSnapshotSyncId);

                    snapshotSender.reset();
                    fsm.getAckReader().markSnapshotSyncInfoOngoing(forcedSnapshotSync, transitionSyncId);
                    return inSnapshotSyncState;
                }

                log.warn("Ignoring Sync Cancel for eventId {}, while running snapshot sync for {}",
                        event.getMetadata().getSyncId(), transitionSyncId);
                return this;
            case REPLICATION_STOP:
                // No need to validate transitionId as REPLICATION_STOP comes either from enforceSnapshotSync or when
                // the runtime FSM transitions back to VERIFYING_REMOTE_LEADER from REPLICATING state
                cancelSnapshotSync("of a request to stop replication.");
                return fsm.getStates().get(LogReplicationStateType.INITIALIZED);
            case REPLICATION_SHUTDOWN:
                cancelSnapshotSync("replication terminated.");
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
        // Used by the SNAPSHOT_SYNC_REQUEST handling above to distinguish a genuinely fresh
        // request from one arriving as part of an ongoing restart storm.
        lastEntryTimeMs = System.currentTimeMillis();
        try {
            // If the transition is to itself, the snapshot sync is continuing, no need to reset the sender
            if (from != this) {
                fsm.getAckReader().setSyncType(SyncType.SNAPSHOT);
                snapshotSender.reset();
                fsm.getAckReader().markSnapshotSyncInfoOngoing(forcedSnapshotSync, transitionSyncId);
                snapshotSyncTransferTimerSample = MeterRegistryProvider.getInstance().map(Timer::start);
            }
            // Consume any pending backoff (set by a preceding SYNC_CANCEL) before transmitting again.
            final long backoffMs = retryBackoffMs;
            retryBackoffMs = 0;
            transmitFuture = fsm.getLogReplicationFSMWorkers()
                    .submit(() -> {
                        if (backoffMs > 0) {
                            log.info("Backing off {}ms before retrying snapshot sync {} (consecutive cancellations={}, remote={})",
                                    backoffMs, transitionSyncId, consecutiveCancellations, fsm.getAckReader().getRemoteClusterId());
                            backoffBeforeRetry(backoffMs);
                        }
                        snapshotSender.transmit(transitionSyncId, forcedSnapshotSync);
                    });
        } catch (Throwable t) {
            log.error("Error on entry of InSnapshotSyncState.", t);
        }
    }

    /**
     * Sleep for up to delayMs, checking periodically whether the snapshot sync has since been
     * stopped/canceled (e.g. by a newer request or a shutdown) so the backoff does not needlessly
     * delay a legitimate subsequent cancellation.
     */
    private void backoffBeforeRetry(long delayMs) {
        long deadline = System.currentTimeMillis() + delayMs;
        long remaining;
        while ((remaining = deadline - System.currentTimeMillis()) > 0 && !snapshotSender.getStopSnapshotSync().get()) {
            try {
                TimeUnit.MILLISECONDS.sleep(Math.min(200, remaining));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Record a cancellation and compute/store the backoff to apply before the next retry attempt,
     * growing exponentially from {@link #INITIAL_RETRY_BACKOFF_MS} up to {@link #MAX_RETRY_BACKOFF_MS}.
     * Package-private (not private) so {@link WaitSnapshotApplyState}, which can also route a
     * cancellation back into this state (a cancel arriving while waiting for snapshot apply to
     * complete), goes through the same accounting instead of restarting immediately and bypassing
     * the backoff.
     *
     * @return the computed backoff, in milliseconds
     */
    long registerCancellationAndComputeBackoff() {
        consecutiveCancellations++;
        long backoff = Math.min(INITIAL_RETRY_BACKOFF_MS << Math.min(consecutiveCancellations - 1, MAX_BACKOFF_SHIFT),
                MAX_RETRY_BACKOFF_MS);
        retryBackoffMs = backoff;
        return backoff;
    }

    @Override
    public void onExit(LogReplicationState to) {
        if (to.getType().equals(LogReplicationStateType.WAIT_SNAPSHOT_APPLY)) {
            // Snapshot transfer succeeded: clear any backoff/cancellation history so a future,
            // unrelated failure starts counting from a clean slate rather than an inflated backoff.
            consecutiveCancellations = 0;
            retryBackoffMs = 0;
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
        snapshotSender.stop();
        snapshotSender.getDataSenderBufferManager().getPendingMessages().clear();
        if (!transmitFuture.isDone()) {
            try {
                transmitFuture.get();
            } catch (Exception e) {
                log.warn("Exception while waiting on snapshot sync to complete.", e);
            }
        }
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
