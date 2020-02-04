package org.corfudb.logreplication.fsm;

/**
 * A class that represents the stopped state of the log replication state machine.
 *
 * The stopped state is a
 */
public class StoppedState implements LogReplicationState {


    public StoppedState () {
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        return null;
    }

    @Override
    public void onEntry(LogReplicationState from) {

    }

    @Override
    public void onExit(LogReplicationState to) {

    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.STOPPED;
    }
}
