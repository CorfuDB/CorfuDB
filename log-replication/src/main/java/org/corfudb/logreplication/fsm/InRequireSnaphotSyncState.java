package org.corfudb.logreplication.fsm;

/**
 * A class that represents the 'In Require Snapshot Sync' state of the Log Replication FSM.
 *
 * In this state we are waiting for a signal to start snapshot sync, as it was determined as required by
 * the source site.
 */
public class InRequireSnaphotSyncState implements LogReplicationState {

    LogReplicationContext context;

    public InRequireSnaphotSyncState(LogReplicationContext context) {
        this.context = context;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                return new InSnapshotSyncState(context);
            case LOG_REPLICATION_STOP:
                return new StoppedState(context);
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
        return LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC;
    }
}
