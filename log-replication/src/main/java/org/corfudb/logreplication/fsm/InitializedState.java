package org.corfudb.logreplication.fsm;

import lombok.extern.slf4j.Slf4j;

/**
 * A class that represents the init state of the Log Replication FSM.
 *
 */
@Slf4j
public class InitializedState implements LogReplicationState {

    LogReplicationFSM fsm;

    public InitializedState(LogReplicationFSM logReplicationFSM) {
        this.fsm = logReplicationFSM;
    }

    /**
     * Process an event.
     *
     * @param event The LogReplicationEvent to process.
     * @return next LogReplicationEvent to transition to.
     */
    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                LogReplicationState snapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                snapshotSyncState.setTransitionEventId(event.getEventID());
                return snapshotSyncState;
            case REPLICATION_START:
                LogReplicationState logEntrySyncState = fsm.getStates()
                        .get(LogReplicationStateType.IN_LOG_ENTRY_SYNC);
                // Set the transition event Id so it can uniquely identify the state and correlate with error events (trim)
                logEntrySyncState.setTransitionEventId(event.getEventID());
                return logEntrySyncState;
            case REPLICATION_STOP:
                return this;
            case REPLICATION_TERMINATED:
                return fsm.getStates().get(LogReplicationStateType.STOPPED);
            default: {
                log.warn("Unexpected log replication event {} when in initialized state.", event.getType());
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
        return LogReplicationStateType.INITIALIZED;
    }
}
