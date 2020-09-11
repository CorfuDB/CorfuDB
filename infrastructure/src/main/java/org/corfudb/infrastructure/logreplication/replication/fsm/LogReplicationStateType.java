package org.corfudb.infrastructure.logreplication.replication.fsm;

/**
 * Types of log replication states.
 *
 * Log Replication process can be in one of the following states.
 */
public enum LogReplicationStateType {
    INITIALIZED,                    // Represents the init state of log replication FSM
    IN_SNAPSHOT_SYNC,               // Represents the state in which snapshot sync (full-sync) is being performed
    WAIT_SNAPSHOT_APPLY,            // Represents the state in which sender is waiting for snapshot sync to be completed (applied) by the receive
    IN_LOG_ENTRY_SYNC,              // Represents the state in which log entry sync (delta-sync) is being performed
    STOPPED                         // Represents the state where the FSM is stopped.
}
