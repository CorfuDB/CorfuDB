package org.corfudb.logreplication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.transmitter.LogEntryTransmitter;

import java.util.UUID;
import java.util.concurrent.Future;

/**
 * A class that represents the InLogEntrySync state of the Log Replication FSM.
 *
 * In this state incremental (delta) updates are being synced to the remote site.
 */
@Slf4j
public class InLogEntrySyncState implements LogReplicationState {

    private LogReplicationFSM fsm;

    private LogEntryTransmitter logEntryTransmitter;

    private UUID logEntrySyncEventId;

    private Future<?> logEntrySyncFuture;

    /**
     * Constructor
     *
     * @param logReplicationFSM log replication finite state machine
     * @param logEntryTransmitter
     */
    public InLogEntrySyncState(LogReplicationFSM logReplicationFSM, LogEntryTransmitter logEntryTransmitter) {
        this.fsm = logReplicationFSM;
        this.logEntryTransmitter = logEntryTransmitter;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                /*
                  Cancel log entry transmit if still in progress, if transmit cannot be canceled
                  we cannot transition to the new state, as there is no guarantee that upper layers
                  can distinguish between a log entry transmit and a snapshot transmit.
                 */
                if (cancelLogEntrySync(event.getType(), "snapshot transmit request.")) {
                    LogReplicationState snapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                    snapshotSyncState.setTransitionEventId(event.getEventID());
                    return snapshotSyncState;
                }
                return this;
            case TRIMMED_EXCEPTION:
                // If trim was intended for current snapshot sync task, cancel and transition to new state
                if (logEntrySyncEventId == event.getEventID()) {
                    /*
                    Cancel log entry sync, if it is still in progress. If transmit cannot be canceled
                    we cannot transition to the new state. In this case it should be canceled as a TrimmedException
                    occurred.
                    */
                    if (cancelLogEntrySync(event.getType(), "trimmed exception.")) {
                        return fsm.getStates().get(LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC);
                    }
                    return this;
                }

                log.warn("Trimmed exception for eventId {}, but running log entry sync for {}",
                        event.getEventID(), logEntrySyncEventId);
                return this;
            case REPLICATION_STOP:
                /*
                  Cancel log entry transmit if still in progress, if transmit cannot be canceled
                  we cannot transition to the new state.
                 */
                return cancelLogEntrySync(event.getType(), "replication being stopped.") ?
                        fsm.getStates().get(LogReplicationStateType.INITIALIZED) : this;
            case REPLICATION_TERMINATED:
                /*
                  Cancel log entry transmit if still in progress, if transmit cannot be canceled
                  we cannot transition to the new state.
                 */
                return cancelLogEntrySync(event.getType(), "replication terminated.") ?
                        fsm.getStates().get(LogReplicationStateType.STOPPED) : this;
            default: {
                log.warn("Unexpected log replication event {} when in log entry transmit state.", event.getType());
            }
        }
        return this;
    }

    /**
     * Cancel log entry transmit task.
     *
     * @param cancelCause cancel cause specific to the caller for debug.
     * @return True, if task was canceled. False, otherwise.
     */
    private boolean cancelLogEntrySync(LogReplicationEvent.LogReplicationEventType type, String cancelCause) {
        // Cancel log entry transmit if still in progress
        if (logEntrySyncFuture != null && !logEntrySyncFuture.isDone()) {
            boolean cancel = logEntrySyncFuture.cancel(true);
            // Verify if task could not be canceled due to normal completion.
            if (!cancel && !logEntrySyncFuture.isDone()) {
                log.error("Log Entry transmit in progress could not be canceled. Cannot process event {}", type);
                return false;
            }
        }
        log.info("Log Entry transmit has been canceled due to {}", cancelCause);
        return true;
    }

    @Override
    public void onEntry(LogReplicationState from) {
        // Execute snapshot transaction for every table to be replicated
        try {
            logEntrySyncFuture = fsm.getStateMachineWorker().submit(logEntryTransmitter::transmit);
        } catch (Throwable t) {
            log.error("Error on entry of InLogEntrySyncState", t);
        }
    }

    @Override
    public void onExit(LogReplicationState to) {
        log.debug("Exiting InLogEntrySyncState");
    }

    @Override
    public void setTransitionEventId(UUID eventId) {
        logEntrySyncEventId = eventId;
    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.IN_LOG_ENTRY_SYNC;
    }
}
