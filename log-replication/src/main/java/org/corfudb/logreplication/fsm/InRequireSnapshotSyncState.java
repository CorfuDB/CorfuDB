package org.corfudb.logreplication.fsm;

import lombok.extern.slf4j.Slf4j;

/**
 * This class represents the InRequireSnapshotSync state of the Log Replication State Machine.
 *
 * This state is entered after a cancel or error (trim), and awaits for a signal to start snapshot sync again.
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
            case REPLICATION_SHUTDOWN:
                return fsm.getStates().get(LogReplicationStateType.STOPPED);
            default: {
                log.warn("Unexpected log replication event {} when in require snapshot transmit state.", event.getType());
            }
        }
        return this;
    }

    @Override
    public void onEntry(LogReplicationState from) {
        // TODO: since a SNAPSHOT_SYNC_REQUEST is the only event that can take us out of this state,
        //  we need a scheduler to re-notify the remote site of the error, in case the request was lost.
    }

    @Override
    public void onExit(LogReplicationState to) {

    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC;
    }
}
