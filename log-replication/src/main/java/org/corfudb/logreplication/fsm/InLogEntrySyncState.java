package org.corfudb.logreplication.fsm;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.transmitter.LogEntryTransmitter;

import java.util.UUID;
import java.util.concurrent.Future;

/**
 * This class represents the InLogEntrySync state of the  Log Replication State Machine.
 *
 * In this state, incremental (delta) updates are being synced to the remote site.
 */
@Slf4j
public class InLogEntrySyncState implements LogReplicationState {

    private LogReplicationFSM fsm;
    // For testing purposes
    @Getter
    private UUID logEntrySyncEventId;
    private LogEntryTransmitter logEntryTransmitter;
    private Future<?> logEntrySyncFuture;

    /**
     * Constructor
     *
     * @param logReplicationFSM log replication finite state machine
     * @param logEntryTransmitter instance that will manage reading and sending log data between sites.
     */
    public InLogEntrySyncState(LogReplicationFSM logReplicationFSM, LogEntryTransmitter logEntryTransmitter) {
        this.fsm = logReplicationFSM;
        this.logEntryTransmitter = logEntryTransmitter;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                cancelLogEntrySync("snapshot transmit request.");
                // TODO (Anny): confirm if we block until task is actually finished?
//                if(!logEntrySyncFuture.isDone()) {
//                    try {
//                        logEntrySyncFuture.get();
//                    } catch (Exception e) {
//                        // Nothing
//                    }
//                }
                LogReplicationState snapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                snapshotSyncState.setTransitionEventId(event.getEventID());
                return snapshotSyncState;
            case TRIMMED_EXCEPTION:
                // If trim was intended for current snapshot sync task, cancel and transition to new state
                if (logEntrySyncEventId == event.getEventID()) {
                    cancelLogEntrySync("trimmed exception.");
                    return fsm.getStates().get(LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC);
                }

                log.warn("Trimmed exception for eventId {}, but running log entry sync for {}",
                        event.getEventID(), logEntrySyncEventId);
                return this;
            case REPLICATION_STOP:
                cancelLogEntrySync("replication being stopped.");
                return fsm.getStates().get(LogReplicationStateType.INITIALIZED);
            case REPLICATION_SHUTDOWN:
                cancelLogEntrySync("replication terminated.");
                return fsm.getStates().get(LogReplicationStateType.STOPPED);
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
     */
    private void cancelLogEntrySync(String cancelCause) {
        // Stop log entry transmit.
        // We can tolerate the last cycle of the transmit being executed before stopped,
        // as snapshot sync is triggered by the app which handles separate listeners
        // for log entry sync and snapshot sync (app can handle this)
        logEntryTransmitter.stop();
        log.info("Log Entry transmit has been canceled due to {}", cancelCause);
    }

    @Override
    public void onEntry(LogReplicationState from) {
        // Execute snapshot transaction for every table to be replicated
        try {
            logEntrySyncFuture = fsm.getStateMachineWorkers().submit(logEntryTransmitter::transmit);
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
