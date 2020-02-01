package org.corfudb.logreplication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.transmitter.LogEntryReader;

/**
 * A class that represents the 'In Log Entry Sync' state of the Log Replication FSM.
 *
 * In this state incremental (delta) updates are being synced to the remote site.
 */
@Slf4j
public class InLogEntrySyncState implements LogReplicationState {

    LogReplicationContext context;

    LogEntryReader logEntryReader;

    public InLogEntrySyncState(LogReplicationContext context) {
        this.context = context;
        logEntryReader = new LogEntryReader(context);
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                return new InSnapshotSyncState(context);
            case TRIMMED_EXCEPTION:
                return new InRequireSnapshotSyncState(context);
            case REPLICATION_STOP:
                return new InitializedState(context);
            default: {
                log.warn("Unexpected log replication event {} when in log entry sync state.", event.getType());
            }
        }
        return this;
    }

    @Override
    public void onEntry(LogReplicationState from) {
        // Execute snapshot transaction for every table to be replicated
        try {
            context.getBlockingOpsScheduler().submit(logEntryReader::sync);
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
