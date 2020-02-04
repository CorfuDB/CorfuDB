package org.corfudb.logreplication.fsm;

/**
 * Interface of a LogReplicationState transition
 */
public interface LogReplicationTransition {
    /**
     * Method to handle Log Replication state transitions.
     *
     * @param from from LogReplicationState.
     * @param to to LogReplicationState.
     */
    default void onTransition(LogReplicationState from, LogReplicationState to) {
        if (from == to) {
            // Do nothing if transition is to the same state.
            return;
        }

        from.onExit(to);

        to.onEntry(from);
    }
}
