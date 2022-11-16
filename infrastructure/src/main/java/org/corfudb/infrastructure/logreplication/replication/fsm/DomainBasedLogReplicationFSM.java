public class DomainBasedLogReplicationFSM extends AbstractLogReplicationFSM {

    void initializeStates() {

        // These individual states will be extended to support incoming event DOMAIN_DESTINATION_UPDATE
        states.put(LogReplicationStateType.INITIALIZED, new InitializedState(this));
        states.put(LogReplicationStateType.IN_SNAPSHOT_SYNC, new InSnapshotSyncState(this, snapshotSender));
        states.put(LogReplicationStateType.WAIT_SNAPSHOT_APPLY, new WaitSnapshotApplyState(this, dataSender, tableManagerPlugin));
        states.put(LogReplicationStateType.IN_LOG_ENTRY_SYNC, new InLogEntrySyncState(this, logEntrySender));
        states.put(LogReplicationStateType.ERROR, new ErrorState(this));
    }
}