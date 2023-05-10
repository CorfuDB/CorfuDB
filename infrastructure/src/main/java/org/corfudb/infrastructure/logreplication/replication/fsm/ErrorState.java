package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SyncStatus;

/**
 * This class represents the stopped state of the Log Replication State Machine.
 *
 * This a termination state in the case of unrecoverable errors.
 **/
@Slf4j
public class ErrorState implements LogReplicationState {

    private final LogReplicationFSM fsm;

    public ErrorState(LogReplicationFSM fsm) {
        this.fsm = fsm;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        return null;
    }

    @Override
    public void onEntry(LogReplicationState from) {
        // Disable periodic sync status periodic task while in initialized state (no actual replication occurring)
        fsm.getAckReader().stopSyncStatusUpdatePeriodicTask();
        fsm.getAckReader().markSyncStatus(SyncStatus.ERROR);
        log.info("Unrecoverable error or explicit shutdown. " +
                "Log Replication is terminated from state {}. To resume, restart the JVM.", from.getType());
    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.ERROR;
    }
}
