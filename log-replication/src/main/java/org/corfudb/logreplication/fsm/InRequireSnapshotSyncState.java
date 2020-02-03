package org.corfudb.logreplication.fsm;

import lombok.extern.slf4j.Slf4j;

/**
 * A class that represents the 'In Require Snapshot Sync' state of the Log Replication FSM.
 *
 * In this state we are waiting for a signal to start snapshot sync, as it was determined as required by
 * the source site.
 */
@Slf4j
public class InRequireSnapshotSyncState implements LogReplicationState {

    LogReplicationContext context;

    public InRequireSnapshotSyncState(LogReplicationContext context) {
        this.context = context;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                return new InSnapshotSyncState(context);
            case REPLICATION_STOP:
                return new InitializedState(context);
            default: {
                log.warn("Unexpected log replication event {} when in require snapshot sync state.", event.getType());
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
