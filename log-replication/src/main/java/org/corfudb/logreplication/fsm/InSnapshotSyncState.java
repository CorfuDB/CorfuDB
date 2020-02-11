package org.corfudb.logreplication.fsm;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.logreplication.transmitter.SnapshotTransmitter;

import java.util.UUID;
import java.util.concurrent.Future;

/**
 * This class represents the InSnapshotSync state of the Log Replication State Machine.
 *
 * In this state full logs are being synced to the remote site, based on a snapshot timestamp.
 */
@Slf4j
public class InSnapshotSyncState implements LogReplicationState {

    private LogReplicationFSM fsm;

    /*
     Uniquely identifies the event that caused the transition to this state.
     This identifier is hold in order to send it back to the application on the snapshotListener
     callback, so it can be correlated to the process that triggered the request.

     This is required in the case that a snapshot sync is canceled and another snapshot sync is requested,
     so the application can discard messages received for the previous snapshot sync, until the new
     request (event) is handled.
     */
    private UUID transitionEventId;

    /*
     Read and transmit a snapshot of the data-store.
     */
    @Getter
    @VisibleForTesting
    private SnapshotTransmitter snapshotTransmitter;

    /*
     A future on the transmit, in case we need to cancel the ongoing snapshot sync.
     */
    private Future<?> transmitFuture;

    /**
     * Constructor
     *
     * @param logReplicationFSM log replication state machine
     * @param snapshotTransmitter snapshot sync transmitter (read and send)
     */
    public InSnapshotSyncState(LogReplicationFSM logReplicationFSM, SnapshotTransmitter snapshotTransmitter) {
        this.fsm = logReplicationFSM;
        this.snapshotTransmitter = snapshotTransmitter;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) throws IllegalLogReplicationTransition {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                /*
                 Cancel ongoing snapshot sync, if it is still in progress.
                 */
                cancelSnapshotSync("another snapshot sync request.");

                /*
                 Set the id of the new snapshot sync request causing the transition.

                 This will be taken onEntry of this state to initiate a snapshot transmit
                 for this given request.
                 */
                setTransitionEventId(event.getEventID());
                snapshotTransmitter.reset();
                return this;
            case SNAPSHOT_SYNC_CONTINUE:
                /*
                 Snapshot sync is broken into multiple tasks, where each task sends a batch of messages
                 corresponding to this snapshot sync. This is done to accommodate the case
                 of multi-site replication sharing a common thread pool, continuation allows to send another
                 batch of updates for the current snapshot sync.
                 */
                if (event.getMetadata().getRequestId() == transitionEventId) {
                    log.debug("Continuation of snapshot sync for {}", event.getEventID());
                    return this;
                } else {
                    log.warn("Unexpected snapshot sync continue event {} when in snapshot sync state {}.",
                            event.getEventID(), transitionEventId);
                }
            case SNAPSHOT_SYNC_CANCEL:
                // If cancel was intended for current snapshot sync task, cancel and transition to new state
                if (transitionEventId == event.getEventID()) {
                    //Cancel snapshot sync, if it is still in progress.
                    cancelSnapshotSync("a explicit cancel by app.");
                    return fsm.getStates().get(LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC);
                }

                log.warn("Snapshot sync cancel requested for eventId {}, but running snapshot sync for {}",
                        event.getEventID(), transitionEventId);
                return this;
            case TRIMMED_EXCEPTION:
                // If trim was intended for current snapshot sync task, cancel and transition to new state
                if (transitionEventId == event.getMetadata().getRequestId()) {
                    /*
                    Cancel snapshot sync, if it is still in progress. If transmit cannot be canceled
                    we cannot transition to the new state. In this case it should be canceled as a TrimmedException
                    occurred.
                    */
                    cancelSnapshotSync("trimmed exception.");
                    return fsm.getStates().get(LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC);
                }

                log.warn("Trimmed exception for eventId {}, but running snapshot sync for {}",
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
                    cancelSnapshotSync("indication of completeness.");
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
                  Cancel snapshot transmit if still in progress.
                 */
                cancelSnapshotSync("replication terminated.");
                return fsm.getStates().get(LogReplicationStateType.STOPPED);
            default: {
                log.warn("Unexpected log replication event {} when in snapshot sync state.", event.getType());
            }

            throw new IllegalLogReplicationTransition(event.getType(), getType());
        }
    }

    @Override
    public void onEntry(LogReplicationState from) {
        try {
            /*
             If the transition is to itself, the snapshot sync is continuing, no need to reset the transmitter.
             */
            if (from != this) {
                snapshotTransmitter.reset();
            }

            /*
             Start transmit of snapshot sync
             */
            transmitFuture = fsm.getStateMachineWorkers().submit(() -> snapshotTransmitter.transmit(transitionEventId));

        } catch (Throwable t) {
            log.error("Error on entry of InSnapshotSyncState.", t);
        }
    }

    @Override
    public void setTransitionEventId(UUID eventId) {
        this.transitionEventId = eventId;
    }

    /**
     * Force interruption of the ongoing snapshot sync task.
     *
     * @param cancelCause cancel cause description
     * @return True, if the task was successfully canceled. False, otherwise
     */
    private void cancelSnapshotSync(String cancelCause) {
        snapshotTransmitter.stop();
        if (!transmitFuture.isDone()) {
            try {
                transmitFuture.get();
            } catch (Exception e) {
                log.warn("Exception while waiting on snapshot sync to complete.", e);
            }
        }
        log.info("Snapshot sync has been canceled due to {}", cancelCause);
    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.IN_SNAPSHOT_SYNC;
    }
}
