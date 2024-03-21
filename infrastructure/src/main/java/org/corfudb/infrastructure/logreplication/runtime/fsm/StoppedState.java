package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationSourceManager;

/**
 * Log Replication Runtime Stopped State.
 *
 * In this state log replication has been stopped due to loss of leadership.
 *
 * @author amartinezman
 */
@Slf4j
public class StoppedState implements LogReplicationRuntimeState {

    private LogReplicationSourceManager replicationSourceManager;

    public StoppedState(LogReplicationSourceManager replicationSourceManager) {
        this.replicationSourceManager = replicationSourceManager;
    }

    @Override
    public LogReplicationRuntimeStateType getType() {
        return LogReplicationRuntimeStateType.STOPPED;
    }

    @Override
    public LogReplicationRuntimeState processEvent(LogReplicationRuntimeEvent event) {
        // Regardless of the event
        return this;
    }

    @Override
    public void onEntry(LogReplicationRuntimeState from) {
        replicationSourceManager.shutdown();
    }
}
