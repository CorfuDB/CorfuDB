package org.corfudb.logreplication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.transmit.LogEntryTransmitter;

import java.util.UUID;
import java.util.concurrent.Future;

/**
 * This class represents the InLogEntrySync state of the Log Replication State Machine.
 *
 * In this state, incremental (delta) updates are being synced to the remote site.
 */
@Slf4j
public class InLogEntrySyncState implements LogReplicationState {

    /*
     * Log Replication State Machine, used to insert internal events into the queue.
     */
    private LogReplicationFSM fsm;

    /*
     * Log Entry Transmitter, used to read and send incremental updates.
     */
    private LogEntryTransmitter logEntryTransmitter;

    /*
     * A future on the log entry transmit, transmit call.
     */
    private Future<?> logEntrySyncFuture;

    /*
     * Unique Identifier of the event that caused the transition to this state,
     * i.e., current event/request being processed.
     */
    private UUID transitionEventId;

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
    public LogReplicationState processEvent(LogReplicationEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                cancelLogEntrySync("snapshot sync request.");
                LogReplicationState snapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                snapshotSyncState.setTransitionEventId(event.getEventID());
                return snapshotSyncState;
            case TRIMMED_EXCEPTION:
                // If trim was intended for current snapshot sync task, cancel and transition to new state
                if (transitionEventId == event.getMetadata().getRequestId()) {
                    cancelLogEntrySync("trimmed exception.");
                    return fsm.getStates().get(LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC);
                }

                log.warn("Trimmed exception for eventId {}, but running log entry sync for {}",
                        event.getEventID(), transitionEventId);
                return this;
            case REPLICATION_STOP:
                cancelLogEntrySync("replication being stopped.");
                return fsm.getStates().get(LogReplicationStateType.INITIALIZED);
            case REPLICATION_SHUTDOWN:
                cancelLogEntrySync("replication terminated.");
                return fsm.getStates().get(LogReplicationStateType.STOPPED);
            case LOG_ENTRY_SYNC_CONTINUE:
                // Snapshot sync is broken into multiple tasks, where each task sends a batch of messages
                // corresponding to this snapshot sync. This is done to accommodate the case
                // of multi-site replication sharing a common thread pool, continuation allows to send another
                // batch of updates for the current snapshot sync.
                if (event.getMetadata().getRequestId() == transitionEventId) {
                    log.debug("Continuation of log entry sync for {}", event.getEventID());
                    return this;
                } else {
                    log.warn("Unexpected log entry sync continue event {} when in log entry sync state {}.",
                            event.getEventID(), transitionEventId);
                }
            default: {
                log.warn("Unexpected log replication event {} when in log entry sync state.", event.getType());
            }

            throw new IllegalTransitionException(event.getType(), getType());
        }
    }

    /**
     * Cancel log entry sync task.
     *
     * @param cancelCause cancel cause specific to the caller for debug.
     */
    private void cancelLogEntrySync(String cancelCause) {
        // Stop log entry sync.
        // We can tolerate the last cycle of the sync being executed before stopped,
        // as snapshot sync is triggered by the app which handles separate listeners
        // for log entry sync and snapshot sync (app can handle this)
        logEntryTransmitter.stop();
        if (!logEntrySyncFuture.isDone()) {
            try {
                logEntrySyncFuture.get();
            } catch (Exception e) {
                log.warn("Exception while waiting on log entry sync to complete.", e);
            }
        }
        log.info("Log Entry sync has been canceled due to {}", cancelCause);
    }

    @Override
    public void onEntry(LogReplicationState from) {
        // Execute snapshot transaction for every table to be replicated
        try {
            logEntrySyncFuture = fsm.getLogReplicationFSMWorkers().submit(logEntryTransmitter::transmit);
        } catch (Throwable t) {
            log.error("Error on entry of InLogEntrySyncState", t);
        }
    }

    @Override
    public void setTransitionEventId(UUID eventId) {
        transitionEventId = eventId;
    }

    @Override
    public UUID getTransitionEventId() {
        return transitionEventId;
    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.IN_LOG_ENTRY_SYNC;
    }
}
