package org.corfudb.logreplication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.transmitter.LogEntryTransmitter;

import java.util.concurrent.Future;

/**
 * A class that represents the 'In Log Entry Sync' state of the Log Replication FSM.
 *
 * In this state incremental (delta) updates are being synced to the remote site.
 */
@Slf4j
public class InLogEntrySyncState implements LogReplicationState {

    private LogReplicationFSM logReplicationFSM;

    private LogEntryTransmitter logEntryTransmitter;

    private Future<?> logEntrySyncFuture;

    public InLogEntrySyncState(LogReplicationFSM logReplicationFSM, LogEntryTransmitter logEntryTransmitter) {
        this.logReplicationFSM = logReplicationFSM;
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
                return cancelLogEntrySync(event.getType(), "snapshot transmit request.") ?
                        logReplicationFSM.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC) : this;
            case TRIMMED_EXCEPTION:
                /*
                  Cancel log entry transmit if still in progress, if transmit cannot be canceled
                  we cannot transition to the new state.
                 */
                return cancelLogEntrySync(event.getType(), "trimmed exception.") ?
                        logReplicationFSM.getStates().get(LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC) : this;
            case REPLICATION_STOP:
                /*
                  Cancel log entry transmit if still in progress, if transmit cannot be canceled
                  we cannot transition to the new state.
                 */
                return cancelLogEntrySync(event.getType(), "replication being stopped.") ?
                        logReplicationFSM.getStates().get(LogReplicationStateType.INITIALIZED) : this;
            case REPLICATION_TERMINATED:
                /*
                  Cancel log entry transmit if still in progress, if transmit cannot be canceled
                  we cannot transition to the new state.
                 */
                return cancelLogEntrySync(event.getType(), "replication terminated.") ?
                        logReplicationFSM.getStates().get(LogReplicationStateType.STOPPED) : this;
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
            logEntrySyncFuture = logReplicationFSM.getStateMachineWorker().submit(logEntryTransmitter::transmit);
        } catch (Throwable t) {
            log.error("Error on entry of InLogEntrySyncState", t);
        }
    }

    @Override
    public void onExit(LogReplicationState to) {
        log.debug("Exiting InLogEntrySyncState");
    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.IN_LOG_ENTRY_SYNC;
    }
}
