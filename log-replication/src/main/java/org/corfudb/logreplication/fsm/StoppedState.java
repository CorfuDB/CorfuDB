package org.corfudb.logreplication.fsm;

import lombok.extern.slf4j.Slf4j;

/**
 * This class represents the stopped state of the Log Replication State Machine.
 *
 * This a termination state in the case of unrecoverable errors.
 **/
@Slf4j
public class StoppedState implements LogReplicationState {


    public StoppedState () {
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        return null;
    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.STOPPED;
    }
}
