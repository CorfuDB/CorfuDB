package org.corfudb.logreplication.fsm;

/**
 * A class that represents the init state of the Log Replication FSM.
 */
public class InitializedState implements LogReplicationState {

    LogReplicationContext context;

    public InitializedState(LogReplicationContext context) {
        this.context = context;
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
                return new InSnapshotSyncState(context);
            case START_LOG_ENTRY_SYNC:
                return new InLogEntrySyncState(context);
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
        // Validating that same replication tables are on both site is business logic, right?

        // create/open Log Replication metadata map and update (reset?) PersistedReplicationMetadata
        // lastSentBaseSnapshotTimestamp =
        // lastAckedTimestamp =
    }

    @Override
    public void onExit(LogReplicationState to) {
    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.INITIALIZED;
    }
}
