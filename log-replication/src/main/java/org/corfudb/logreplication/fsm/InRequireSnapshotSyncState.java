package org.corfudb.logreplication.fsm;

import lombok.extern.slf4j.Slf4j;

/**
 * A class that represents the 'In Require Snapshot Sync' state of the Log Replication FSM.
 *
 * In this state we are waiting for a signal to start snapshot transmit, as it was determined as required by
 * the source site.
 */
@Slf4j
public class InRequireSnapshotSyncState implements LogReplicationState {

    LogReplicationFSM fsm;

    public InRequireSnapshotSyncState(LogReplicationFSM logReplicationFSM) {
        this.fsm = logReplicationFSM;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                LogReplicationState snapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                snapshotSyncState.setTransitionEventId(event.getEventID());
                return snapshotSyncState;
            case REPLICATION_STOP:
                return fsm.getStates().get(LogReplicationStateType.INITIALIZED);
            case REPLICATION_TERMINATED:
                return fsm.getStates().get(LogReplicationStateType.STOPPED);
            default: {
                log.warn("Unexpected log replication event {} when in require snapshot transmit state.", event.getType());
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
