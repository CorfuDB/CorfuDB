package org.corfudb.logreplication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.transmitter.LogEntryReader;

import java.util.concurrent.Future;

/**
 * A class that represents the 'In Log Entry Sync' state of the Log Replication FSM.
 *
 * In this state incremental (delta) updates are being synced to the remote site.
 */
@Slf4j
public class InLogEntrySyncState implements LogReplicationState {

    private LogReplicationContext context;

    private LogEntryReader logEntryReader;

    private Future<?> logEntrySyncFuture;

    public InLogEntrySyncState(LogReplicationContext context) {
        this.context = context;
        logEntryReader = new LogEntryReader(context);
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                /*
                  Cancel log entry sync if still in progress, if sync cannot be canceled
                  we cannot transition to the new state, as there is no guarantee that upper layers
                  can distinguish between a log entry sync and a snapshot sync.
                 */
                return cancelLogEntrySync("snapshot sync request.") ?
                        new InSnapshotSyncState(context) : this;
            case TRIMMED_EXCEPTION:
                return new InRequireSnapshotSyncState(context);
            case REPLICATION_STOP:
                /*
                  Cancel log entry sync if still in progress, if sync cannot be canceled
                  we cannot transition to the new state.
                 */
                return cancelLogEntrySync("replication being stopped.") ?
                        new InitializedState(context) : this;
            default: {
                log.warn("Unexpected log replication event {} when in log entry sync state.", event.getType());
            }
        }
        return this;
    }

    private boolean cancelLogEntrySync(String cancelCause) {
        // Cancel log entry sync if still in progress
        if (logEntrySyncFuture != null && !logEntrySyncFuture.isDone()) {
            boolean cancel = logEntrySyncFuture.cancel(true);
            // Verify if task could not be canceled due to normal completion.
            if (!cancel && !logEntrySyncFuture.isDone()) {
                log.error("Log Entry sync in progress could not be canceled.");
                return false;
            }
        }
        log.info("Log Entry sync has been canceled due to {}", cancelCause);
        return true;
    }

    @Override
    public void onEntry(LogReplicationState from) {
        // Execute snapshot transaction for every table to be replicated
        try {
            logEntrySyncFuture = context.getStateMachineWorker().submit(logEntryReader::sync);
        } catch (Throwable t) {
            // Log Error
        }
    }

    @Override
    public void onExit(LogReplicationState to) {

    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.IN_LOG_ENTRY_SYNC;
    }
}
