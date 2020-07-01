package org.corfudb.infrastructure.logreplication.replication.fsm;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.replication.send.SnapshotSender;

import java.util.UUID;
import java.util.concurrent.Future;

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

    /**
     Uniquely identifies the event that caused the transition to this state.
     This identifier is hold in order to send it back to the application through the DataSender
     callback, so it can be correlated to the process that triggered the request.

     This is required in the case that a snapshot sync is canceled and another snapshot sync is requested,
     so the application can discard messages received for the previous snapshot sync, until the new
     request (event) is handled.
     */
    private UUID transitionEventId;

    /**
     * Read and send a snapshot of the data-store.
     */
    @Getter
    @VisibleForTesting
    private SnapshotSender snapshotSender;

    /**
     * A future on the send, in case we need to cancel the ongoing snapshot sync.
     */
    private Future<?> transmitFuture;

    /**
     * Constructor
     *
     * @param logReplicationFSM log replication state machine
     * @param snapshotSender snapshot sync send (read and send)
     */
    public InSnapshotSyncState(LogReplicationFSM logReplicationFSM, SnapshotSender snapshotSender) {
        this.fsm = logReplicationFSM;
        this.snapshotSender = snapshotSender;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                /*
                 Cancel ongoing snapshot sync, if it is still in progress.
                 */
                cancelSnapshotSync("another snapshot sync request.");

                /*
                 Set the id of the new snapshot sync request causing the transition.

                 This will be taken onEntry of this state to initiate a snapshot send
                 for this given request.
                 */
                setTransitionEventId(event.getEventID());
                snapshotSender.reset();
                return this;
            case SNAPSHOT_SYNC_CONTINUE:
                /*
                 Snapshot sync is broken into multiple tasks, where each task sends a batch of messages
                 corresponding to this snapshot sync. This is done to accommodate the case
                 of multi-cluster replication sharing a common thread pool, continuation allows to send another
                 batch of updates for the current snapshot sync.
                 */
                if (event.getMetadata().getRequestId() == transitionEventId) {
                    log.debug("InSnapshotSync[{}] :: Continuation of snapshot sync for {}", this, event.getEventID());
                    return this;
                } else {
                    log.warn("Unexpected snapshot sync continue event {} when in snapshot sync state {}.",
                            event.getEventID(), transitionEventId);
                    throw new IllegalTransitionException(event.getType(), getType());
                }
            case SYNC_CANCEL:
                // If cancel was intended for current snapshot sync task, cancel and transition to new state
                if (transitionEventId == event.getMetadata().getRequestId()) {
                    cancelSnapshotSync("cancellation request.");
                    // Re-trigger SnapshotSync due to error, generate a new event Id for the new snapshot sync
                    LogReplicationState inSnapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                    inSnapshotSyncState.setTransitionEventId(UUID.randomUUID());
                    snapshotSender.reset();
                    return inSnapshotSyncState;
                }

                log.warn("Sync Cancel for eventId {}, but running snapshot sync for {}",
                        event.getEventID(), transitionEventId);
                return this;
            case SNAPSHOT_SYNC_COMPLETE:
                UUID snapshotSyncRequestId = event.getMetadata().getRequestId();
                /*
                 This is required as in the following sequence of events:

                 1. SNAPSHOT_SYNC (ID = 1) EXTERNAL
                 2. SNAPSHOT_CANCEL (ID = 1) EXTERNAL
                 3. SNAPSHOT_SYNC (ID = 2) EXTERNAL

                 Snapshot Sync with ID = 1 could be completed in between (1 and 2) but show up in the queue
                 as 4, attempting to process a completion event for the incorrect snapshot sync.
                 */
                if (snapshotSyncRequestId == transitionEventId) {
                    cancelSnapshotSync("it has completed.");
                    LogReplicationState logEntrySyncState = fsm.getStates()
                            .get(LogReplicationStateType.IN_LOG_ENTRY_SYNC);
                    // We need to set a new transition event Id, so anything happening on this new state
                    // is marked with this unique Id and correlated to cancel or trimmed events.
                    logEntrySyncState.setTransitionEventId(event.getEventID());
                    return logEntrySyncState;
                }

                log.warn("Ignoring snapshot sync complete event, for request {}, as ongoing snapshot sync is {}",
                        snapshotSyncRequestId, transitionEventId);
                return this;
            case REPLICATION_STOP:
                /*
                  Cancel snapshot sync if still in progress.
                 */
                 cancelSnapshotSync("request to stop replication.");
                 return fsm.getStates().get(LogReplicationStateType.INITIALIZED);
            case REPLICATION_SHUTDOWN:
                /*
                  Cancel snapshot send if still in progress.
                 */
                cancelSnapshotSync("replication terminated.");
                return fsm.getStates().get(LogReplicationStateType.STOPPED);
            default: {
                log.warn("Unexpected log replication event {} when in snapshot sync state.", event.getType());
            }

            throw new IllegalTransitionException(event.getType(), getType());
        }
    }

    @Override
    public void onEntry(LogReplicationState from) {
        try {
            /*
             If the transition is to itself, the snapshot sync is continuing, no need to reset the send.
             */
            if (from != this) {
                snapshotSender.reset();
            }

            /*
             Start send of snapshot sync
             */
            transmitFuture = fsm.getLogReplicationFSMWorkers().submit(() -> snapshotSender.transmit(transitionEventId));
        } catch (Throwable t) {
            log.error("Error on entry of InSnapshotSyncState.", t);
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
     * @return True, if the task was successfully canceled. False, otherwise
     */
    private void cancelSnapshotSync(String cancelCause) {
        snapshotSender.stop();
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
}
