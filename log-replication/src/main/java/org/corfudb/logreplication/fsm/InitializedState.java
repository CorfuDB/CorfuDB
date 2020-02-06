package org.corfudb.logreplication.fsm;

import lombok.extern.slf4j.Slf4j;

/**
 * This class represents the Init state of the Log Replication State Machine.
 *
 * On FSM start this is the default state, there are two events that enable transitions:
 * (1) SNAPSHOT_SYNC_REQUEST: external event (application driven) indicating to start full sync at a given snapshot.
 * (2) REPLICATION_START: external event (application driven) indicating that connectivity to remote site has been
 *                        established and the replication can start.
 */
@Slf4j
public class InitializedState implements LogReplicationState {

    /*
     * Log Replication Finite State Machine Instance
     */
    private LogReplicationFSM fsm;

    /**
     * Constructor
     *
     * @param logReplicationFSM log replication finite state machine
     */
    public InitializedState(LogReplicationFSM logReplicationFSM) {
        this.fsm = logReplicationFSM;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                // Set the id of the event that caused the transition to the new state
                // This is used to correlate trim or error events that derive from this state
                LogReplicationState snapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                snapshotSyncState.setTransitionEventId(event.getEventID());
                return snapshotSyncState;
            case REPLICATION_START:
                // Set the id of the event that caused the transition to the new state
                // This is used to correlate trim or error events that derive from this state
                LogReplicationState logEntrySyncState = fsm.getStates().get(LogReplicationStateType.IN_LOG_ENTRY_SYNC);
                logEntrySyncState.setTransitionEventId(event.getEventID());
                return logEntrySyncState;
            case REPLICATION_STOP:
                return this;
            case REPLICATION_SHUTDOWN:
                return fsm.getStates().get(LogReplicationStateType.STOPPED);
            default: {
                log.warn("Unexpected log replication event {} when in initialized state.", event.getType());
            }
        }
        return this;
    }

    @Override
    public void onEntry(LogReplicationState from) {
        /*
         * There is no automatic transition from this state, external signals to start snapshot sync or
         * to start replication (upon connectivity to remote site) are required.
         */
    }

    @Override
    public void onExit(LogReplicationState to) {
    }

    @Override
    public LogReplicationStateType getType() { return LogReplicationStateType.INITIALIZED; }
}
