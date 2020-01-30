package org.corfudb.logreplication.fsm;

public class StoppedState implements LogReplicationState {

    LogReplicationContext context;

    public StoppedState (LogReplicationContext context) {
        this.context = context;
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
