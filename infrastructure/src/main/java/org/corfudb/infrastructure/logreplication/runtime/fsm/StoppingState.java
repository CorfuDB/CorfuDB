package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationSourceManager;

@Slf4j
public class StoppingState implements LogReplicationRuntimeState {

    LogReplicationSourceManager replicationSourceManager;

    public StoppingState(LogReplicationSourceManager replicationSourceManager) {
        this.replicationSourceManager = replicationSourceManager;
    }

    @Override
    public LogReplicationRuntimeStateType getType() {
        return LogReplicationRuntimeStateType.STOPPING;
    }

    @Override
    public LogReplicationRuntimeState processEvent(LogReplicationRuntimeEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            default: {
                log.warn("Unexpected communication event {} when in init state.", event.getType());
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(LogReplicationRuntimeState from) {
        replicationSourceManager.shutdown();
    }
}
