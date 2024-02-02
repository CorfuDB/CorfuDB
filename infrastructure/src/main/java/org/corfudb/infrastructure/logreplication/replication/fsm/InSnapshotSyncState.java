package org.corfudb.infrastructure.logreplication.replication.fsm;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.logreplication.replication.send.SnapshotSender;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.SyncStatus;
import org.corfudb.runtime.LogReplication.SyncType;

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
                    // Re-trigger SnapshotSync due to error, generate a new event Id for the new snapshot sync
                    LogReplicationState inSnapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                    UUID newSnapshotSyncId = UUID.randomUUID();
                    log.debug("Starting new snapshot sync after cancellation id={}", newSnapshotSyncId);
                    inSnapshotSyncState.setTransitionSyncId(newSnapshotSyncId);
                    // If a force snapshot sync gets cancelled due to ACK timeout, a new snapshot sync is triggered.
                    // Retain the 'forced' information in the subsequent snapshot syncs
                    ((InSnapshotSyncState)inSnapshotSyncState).setForcedSnapshotSync(event.getMetadata().isForcedSnapshotSync());
                    snapshotSender.reset();
                    if (event.getMetadata().isTimeoutException()) {
                        requestSnapshotSyncDataForRoutingQModel();
                    }
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
        try {
            // If the transition is to itself, the snapshot sync is continuing, no need to reset the sender
            if (from != this) {
                fsm.getAckReader().setSyncType(SyncType.SNAPSHOT);
                snapshotSender.reset();
                requestSnapshotSyncDataForRoutingQModel();
                fsm.getAckReader().markSnapshotSyncInfoOngoing(forcedSnapshotSync, transitionSyncId);
                snapshotSyncTransferTimerSample = MeterRegistryProvider.getInstance().map(Timer::start);
            }
            transmitFuture = fsm.getLogReplicationFSMWorkers()
                    .submit(() -> snapshotSender.transmit(transitionSyncId, forcedSnapshotSync));
        } catch (Throwable t) {
            log.error("Error on entry of InSnapshotSyncState.", t);
        }
    }

    private void requestSnapshotSyncDataForRoutingQModel() {
        if (fsm.getSession().getSubscriber().getModel() == LogReplication.ReplicationModel.ROUTING_QUEUES) {
            // TODO v2: Evaluate if a new sync id should be generated here
            snapshotSender.requestClientForSnapshotData(transitionSyncId);
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
