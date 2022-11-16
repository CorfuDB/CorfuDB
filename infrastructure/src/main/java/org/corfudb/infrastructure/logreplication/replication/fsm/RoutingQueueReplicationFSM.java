import org.corfudb.infrastructure.logreplication.replication.fsm.AbstractLogReplicationFSM;

public class RoutingQueueReplicationFSM extends AbstractLogReplicationFSM {

    void initializeStates() {

        states.put(LogReplicationStateType.INITIALIZED, new InitializedState(this));
        states.put(LogReplicationStateType.IN_SNAPSHOT_SYNC, new InSnapshotSyncState(this, snapshotSender));
        states.put(LogReplicationStateType.WAIT_SNAPSHOT_APPLY, new WaitSnapshotApplyState(this, dataSender, tableManagerPlugin));

        // New LogEntry Sync state which will write to a table to request for a snapshot sync on hitting a
        // TrimmedException
        // Note: The current FSM already supports an event for SNAPSHOT_SYNC_REQUEST in any state.  Check if the same
        // event can be leveraged
        states.put(LogReplicationStateType.ROUTING_QUEUE_LOG_ENTRY_SYNC, new RoutingQueueLogEntrySyncState(this,
            logEntrySender));
        states.put(LogReplicationStateType.ERROR, new ErrorState(this));
    }
}