package org.corfudb.logreplication.fsm;

public class InRequireSnaphotSyncState implements LogReplicationState {

    LogReplicationContext context;

    public InRequireSnaphotSyncState(LogReplicationContext context) {
        this.context = context;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        switch (event.getType()) {
            case SNAPHOT_SYNC_REQUEST:
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
