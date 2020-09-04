package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.extern.slf4j.Slf4j;

/**
 * This class represents the Init state of the Log Replication State Machine.
 *
 * On FSM start this is the default state, there are three events that cause transitions from this state:
 *
 * (1) SNAPSHOT_SYNC_REQUEST: external event (application driven) indicating to start snapshot/full sync.
 * (2) LOG_ENTRY_SYNC_REQUEST: external event (application driven) indicating that connectivity to remote cluster has been
 *                        established and the replication can start, this enters into log entry/delta sync.
 * (3) REPLICATION_SHUTDOWN: completely stop/terminate log replication.
 */
@Slf4j
public class InitializedState implements LogReplicationState {

    /*
     * Log Replication Finite State Machine Instance
     */
    private final LogReplicationFSM fsm;

    /**
     * Constructor
     *
     * @param logReplicationFSM log replication finite state machine
     */
    public InitializedState(LogReplicationFSM logReplicationFSM) {
        this.fsm = logReplicationFSM;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                log.info("Start Snapshot Sync, requestId={}", event.getEventID());
                // Set the id of the event that caused the transition to the new state
                // This is used to correlate trim or error events that derive from this state
                LogReplicationState snapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                snapshotSyncState.setTransitionEventId(event.getEventID());
                return snapshotSyncState;
            case SNAPSHOT_TRANSFER_COMPLETE:
                log.info("Snapshot Sync transfer completed. Wait for snapshot apply to complete.");
                WaitSnapshotApplyState waitSnapshotApplyState = (WaitSnapshotApplyState)fsm.getStates().get(LogReplicationStateType.WAIT_SNAPSHOT_APPLY);
                waitSnapshotApplyState.setTransitionEventId(event.getEventID());
                waitSnapshotApplyState.setBaseSnapshotTimestamp(fsm.getBaseSnapshot());
                fsm.setBaseSnapshot(event.getMetadata().getLastTransferredBaseSnapshot());
                fsm.setAckedTimestamp(event.getMetadata().getLastLogEntrySyncedTimestamp());
                return waitSnapshotApplyState;
            case LOG_ENTRY_SYNC_REQUEST:
                log.info("Start Log Entry Sync, requestId={}", event.getEventID());
                // Set the id of the event that caused the transition to the new state
                // This is used to correlate trim or error events that derive from this state
                LogReplicationState logEntrySyncState = fsm.getStates().get(LogReplicationStateType.IN_LOG_ENTRY_SYNC);
                logEntrySyncState.setTransitionEventId(event.getEventID());
                fsm.setBaseSnapshot(event.getMetadata().getLastTransferredBaseSnapshot());
                fsm.setAckedTimestamp(event.getMetadata().getLastLogEntrySyncedTimestamp());
                return logEntrySyncState;
            case REPLICATION_STOP:
                return this;
            case REPLICATION_SHUTDOWN:
                return fsm.getStates().get(LogReplicationStateType.STOPPED);
            default: {
                log.warn("Unexpected log replication event {} when in initialized state.", event.getType());
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public LogReplicationStateType getType() { return LogReplicationStateType.INITIALIZED; }
}
