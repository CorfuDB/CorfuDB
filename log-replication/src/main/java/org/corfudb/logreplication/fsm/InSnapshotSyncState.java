package org.corfudb.logreplication.fsm;

import lombok.Setter;
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

    private static final UUID NIL_UUID = new UUID(0,0);

    private LogReplicationFSM fsm;

    /*
     Uniquely identifies the snapshot sync event that caused the transition to this state.
     This identifier is hold in order to send it back to the application on the snapshotListener
     callback, so it can be correlated to the process that triggered the request.

     This is required in the case that a snapshot sync is canceled and another snapshot sync is requested,
     so the application can discard messages received for the previous snapshot sync, until the new
     request (event) is handled.
     */
    @Setter
    private UUID snapshotSyncEventId = NIL_UUID;

    /*
     Read and transmit a snapshot of the data-store.
     */
    private SnapshotTransmitter snapshotTransmitter;

    /*
     A future on the transmit, in case we need to cancel the ongoing snapshot sync.
     */
    private Future<?> transmitFuture;


    /**
     * Constructor
     *
     * @param logReplicationFSM state machine
     * @param snapshotTransmitter transmitter (read & send snapshot across sites)
     */
    public InSnapshotSyncState(LogReplicationFSM logReplicationFSM, SnapshotTransmitter snapshotTransmitter) {
        this.fsm = logReplicationFSM;
        this.snapshotTransmitter = snapshotTransmitter;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        switch (event.getType()) {
            // Case where another snapshot (full) transmit is requested.
            case SNAPSHOT_SYNC_REQUEST:
                /*
                  Cancel snapshot sync, if it is still in progress. If transmit cannot be canceled
                  we cannot transition to the new state.
                 */
                if (cancelSnapshotSync("another snapshot transmit request.")) {
                    /*
                    Set the id of the new snapshot sync request causing the transition.
                    This will be taken onEntry of this state to initiate a snapshot transmit
                    for this given request.
                    */
                    this.setTransitionEventId(event.getEventID());
                }
                return this;
            case SNAPSHOT_SYNC_CANCEL:
                // If cancel was intended for current snapshot sync task, cancel and transition to new state
                if (snapshotSyncEventId == event.getEventID()) {
                    /*
                    Cancel snapshot sync, if it is still in progress. If transmit cannot be canceled
                    we cannot transition to the new state.
                    */
                    if (cancelSnapshotSync("a explicit cancel by app.")) {
                        return fsm.getStates().get(LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC);
                    }
                }

                log.warn("Snapshot sync cancel requested for eventId {}, but running snapshot sync for {}",
                        event.getEventID(), snapshotSyncEventId);
                return this;
            case TRIMMED_EXCEPTION:
                // If trim was intended for current snapshot sync task, cancel and transition to new state
                if (snapshotSyncEventId == event.getEventID()) {
                    /*
                    Cancel snapshot sync, if it is still in progress. If transmit cannot be canceled
                    we cannot transition to the new state. In this case it should be canceled as a TrimmedException
                    occurred.
                    */
                    if (cancelSnapshotSync("trimmed exception.")) {
                        return fsm.getStates().get(LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC);
                    }
                    return this;
                }

                log.warn("Trimmed exception for eventId {}, but running snapshot sync for {}",
                        event.getEventID(), snapshotSyncEventId);
                return this;
            case SNAPSHOT_SYNC_COMPLETE:
                UUID snapshotSyncCompleteId = event.getEventID();

                /*
                 This is required as in the following sequence of events:
                 1. SNAPSHOT_SYNC (ID = 1) EXTERNAL
                 2. SNAPSHOT_CANCEL (ID = 1) EXTERNAL
                 3. SNAPSHOT_SYNC (ID = 2) EXTERNAL

                 Snapshot Sync with ID = 1 could be completed in between (1 and 2) but show up in the queue
                 as 4, attempting to process a completion event for the incorrect snapshot sync.
                 */
                if (snapshotSyncCompleteId == snapshotSyncEventId) {
                    // TODO: Should we really attempt to cancel? If it was completed it should be Done already.
                    // Perhaps log an error?
                    if (cancelSnapshotSync("indication of completeness.")) {
                        LogReplicationState logEntrySyncState = fsm.getStates()
                                .get(LogReplicationStateType.IN_LOG_ENTRY_SYNC);
                        // We need to set a new transition event Id, so anything happening on this new state
                        // is marked with this unique Id and correlated to cancel or trimmed events.
                        logEntrySyncState.setTransitionEventId(UUID.randomUUID());
                        return logEntrySyncState;
                    }
                    return this;
                }

                log.warn("Ignoring snapshot sync complete event, for request {}, as ongoing snapshot sync is {}",
                        snapshotSyncCompleteId, snapshotSyncEventId);
                return this;
            case REPLICATION_STOP:
                /*
                  Cancel snapshot transmit if still in progress, if transmit cannot be canceled
                  we cannot transition to the new state.
                 */
                return cancelSnapshotSync("request to stop replication.") ?
                        fsm.getStates().get(LogReplicationStateType.INITIALIZED) : this;
            case REPLICATION_SHUTDOWN:
                /*
                  Cancel snapshot transmit if still in progress, if transmit cannot be canceled
                  we cannot transition to the new state.
                 */
                return cancelSnapshotSync("replication terminated.") ?
                        fsm.getStates().get(LogReplicationStateType.STOPPED) : this;
            default: {
                log.warn("Unexpected log replication event {} when in snapshot transmit state.", event.getType());
            }
        }
        return this;
    }

    /**
     * Force interruption/canceling of the ongoing snapshot sync task.
     *
     * @param cancelCause cancel cause to add to log message.
     * @return True, if the task was successfully canceled. False, otherwise.
     */
    private boolean cancelSnapshotSync(String cancelCause) {
        // Cancel snapshot transmit if still in progress
        if (transmitFuture != null && !transmitFuture.isDone()) {
            boolean cancel = transmitFuture.cancel(true);
            // Verify if task could not be canceled due to normal completion.
            if (!cancel && !transmitFuture.isDone()) {
                log.error("Snapshot transmit in progress could not be canceled.");
                return false;
            }
        }
        log.info("Snapshot transmit has been canceled due to {}", cancelCause);
        return true;
    }

    @Override
    public void onEntry(LogReplicationState from) {
        // Execute snapshot transaction for every table to be replicated
        try {
            transmitFuture = fsm.getStateMachineWorker().submit(() -> {
                snapshotTransmitter.transmit(snapshotSyncEventId);
            });
        } catch (Throwable t) {
            log.error("Error on entry of InSnapshotSyncState.", t);
        }
    }

    @Override
    public void onExit(LogReplicationState to) {
        // Reset the event Id that caused the transition to this state
        this.snapshotSyncEventId = NIL_UUID;
    }

    @Override
    public void setTransitionEventId(UUID eventId) {
        this.snapshotSyncEventId = eventId;
    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.IN_SNAPSHOT_SYNC;
    }
}
