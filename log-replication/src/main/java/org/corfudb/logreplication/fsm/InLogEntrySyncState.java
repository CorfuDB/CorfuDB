package org.corfudb.logreplication.fsm;

/**
 * A class that represents the 'In Log Entry Sync' state of the Log Replication FSM.
 *
 * In this state incremental (delta) updates are being synced to the remote site.
 */
public class InLogEntrySyncState implements LogReplicationState {

    LogReplicationContext context;

    public InLogEntrySyncState(LogReplicationContext context) {
        this.context = context;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                return new InSnapshotSyncState(context);
            case TRIMMED_EXCEPTION:
                return new InRequireSnaphotSyncState(context);
            case REPLICATION_STOP:
                return new InitializedState(context);
            default: {
                // Log unexpected LogReplicationEvent when in initialized state
            }
        }
        return this;
    }

    @Override
    public void onEntry(LogReplicationState from) {

    }

    @Override
    public void onExit(LogReplicationState to) {

    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.IN_LOG_ENTRY_SYNC;
    }
}
