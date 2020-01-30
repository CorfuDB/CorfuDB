package org.corfudb.logreplication.fsm;

public class LogReplicationTransitionImpl implements LogReplicationTransition {

    /**
     * Log Replication Context, shared elements between states.
     */
    LogReplicationContext context;

    public LogReplicationTransitionImpl(LogReplicationContext context) {
        this.context = context;
    }

    @Override
    public void onTransition(LogReplicationState from, LogReplicationState to) {

        if (from == to) {
            // Do nothing if transition is to the same state.
            return;
        }

        from.onExit(to);

        to.onEntry(from);
    }
}
