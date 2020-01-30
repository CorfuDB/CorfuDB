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
    void onTransition(LogReplicationState from, LogReplicationState to);
}
