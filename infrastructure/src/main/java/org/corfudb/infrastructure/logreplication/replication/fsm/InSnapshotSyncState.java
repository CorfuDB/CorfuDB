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
     * Uniquely identifies the event that caused the transition to this state.
     * This identifier is hold in order to send it back to the application through the DataSender
     * callback, so it can be correlated to the process that triggered the request.
     * <p>
     * This is required in the case that a snapshot sync is canceled and another snapshot sync is requested,
     * so the application can discard messages received for the previous snapshot sync, until the new
     * request (event) is handled.
     */
    private UUID transitionEventId;

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
                /*
                 Cancel ongoing snapshot sync, if it is still in progress.
                 */
                setForcedSnapshotSync(event.getMetadata().isForcedSnapshotSync());
                String cancelCause = forcedSnapshotSync ? "incoming forced snapshot sync." : "another snapshot sync request.";
                cancelSnapshotSync(cancelCause);

                /*
                 Set the id of the new snapshot sync request causing the transition.

                 This will be taken onEntry of this state to initiate a snapshot send
                 for this given request.
                 */
                setTransitionEventId(event.getEventId());
                snapshotSender.reset();
                fsm.getAckReader().markSnapshotSyncInfoOngoing(forcedSnapshotSync, transitionEventId);
                return this;
            case SNAPSHOT_SYNC_CONTINUE:
                /*
                 Snapshot sync is broken into multiple tasks, where each task sends a batch of messages
                 corresponding to this snapshot sync. This is done to accommodate the case
                 of multi-cluster replication sharing a common thread pool, continuation allows to send another
                 batch of updates for the current snapshot sync.
                 */
                if (event.getMetadata().getRequestId() == transitionEventId) {
                    log.debug("InSnapshotSync[{}] :: Continuation of snapshot sync for {}", transitionEventId, event.getEventId());
                    return this;
                } else {
                    log.warn("Unexpected snapshot sync continue event {} when in snapshot sync state {}.",
                            event.getEventId(), transitionEventId);
                    throw new IllegalTransitionException(event.getType(), getType());
                }
            case SNAPSHOT_TRANSFER_COMPLETE:
                log.info("Snapshot Sync transfer is complete for {}", event.getEventId());
                WaitSnapshotApplyState waitSnapshotApplyState = (WaitSnapshotApplyState)fsm.getStates().get(LogReplicationStateType.WAIT_SNAPSHOT_APPLY);
                waitSnapshotApplyState.setTransitionEventId(transitionEventId);
                waitSnapshotApplyState.setBaseSnapshotTimestamp(snapshotSender.getBaseSnapshotTimestamp());
                fsm.setBaseSnapshot(event.getMetadata().getLastTransferredBaseSnapshot());
                fsm.setAckedTimestamp(event.getMetadata().getLastLogEntrySyncedTimestamp());
                snapshotSyncAcksCounter.ifPresent(AtomicLong::getAndIncrement);
                return waitSnapshotApplyState;
            case SYNC_CANCEL:
                // If cancel was intended for current snapshot sync task, cancel and transition to new state
                if (transitionEventId.equals(event.getMetadata().getRequestId())) {
                    cancelSnapshotSync("cancellation request.");
                    // Re-trigger SnapshotSync due to error, generate a new event Id for the new snapshot sync
                    LogReplicationState inSnapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                    UUID newSnapshotSyncId = UUID.randomUUID();
                    log.debug("Starting new snapshot sync after cancellation id={}", newSnapshotSyncId);
                    inSnapshotSyncState.setTransitionEventId(newSnapshotSyncId);
                    ((InSnapshotSyncState)inSnapshotSyncState).setForcedSnapshotSync(false);
                    snapshotSender.reset();
                    fsm.getAckReader().markSnapshotSyncInfoOngoing(forcedSnapshotSync, transitionEventId);
                    return inSnapshotSyncState;
                }

                log.warn("Sync Cancel for eventId {}, but running snapshot sync for {}",
                        event.getEventId(), transitionEventId);
                return this;
            case REPLICATION_STOP:
                cancelSnapshotSync("of a request to stop replication.");
                return fsm.getStates().get(LogReplicationStateType.INITIALIZED);
            case REPLICATION_SHUTDOWN:
                cancelSnapshotSync("replication terminated.");
                return fsm.getStates().get(LogReplicationStateType.ERROR);
            default: {
                log.warn("Unexpected log replication event {} when in snapshot sync state.", event.getType());
            }

            throw new IllegalTransitionException(event.getType(), getType());
        }
    }

    @Override
    public void onEntry(LogReplicationState from) {
        try {
            // If the transition is to itself, the snapshot sync is continuing, no need to reset the sender
            if (from != this) {
                fsm.getAckReader().setSyncType(SyncType.SNAPSHOT);
                snapshotSender.reset();
                fsm.getAckReader().markSnapshotSyncInfoOngoing(forcedSnapshotSync, transitionEventId);
                snapshotSyncTransferTimerSample = MeterRegistryProvider.getInstance().map(Timer::start);
            }
            transmitFuture = fsm.getLogReplicationFSMWorkers()
                    .submit(() -> snapshotSender.transmit(transitionEventId));
        } catch (Throwable t) {
            log.error("Error on entry of InSnapshotSyncState.", t);
        }
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

    @Override
    public void setTransitionEventId(UUID eventId) {
        this.transitionEventId = eventId;
    }

    @Override
    public UUID getTransitionEventId() { return transitionEventId; }

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
