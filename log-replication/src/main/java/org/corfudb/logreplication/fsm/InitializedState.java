package org.corfudb.logreplication.fsm;

import lombok.extern.slf4j.Slf4j;

/**
 * A class that represents the init state of the Log Replication FSM.
 */
@Slf4j
public class InitializedState implements LogReplicationState {

    LogReplicationFSM logReplicationFSM;

    public InitializedState(LogReplicationFSM logReplicationFSM) {
        this.logReplicationFSM = logReplicationFSM;
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
                return logReplicationFSM.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
            case REPLICATION_START:
                return logReplicationFSM.getStates().get(LogReplicationStateType.IN_LOG_ENTRY_SYNC);
            case REPLICATION_STOP:
                return this;
            case REPLICATION_TERMINATED:
                return logReplicationFSM.getStates().get(LogReplicationStateType.STOPPED);
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
