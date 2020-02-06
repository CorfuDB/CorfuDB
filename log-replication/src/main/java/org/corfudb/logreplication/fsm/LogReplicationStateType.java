package org.corfudb.logreplication.fsm;

/**
 * Types of log replication states.
 *
 * Log Replication process can be in one of the following states.
 */
public enum LogReplicationStateType {
    INITIALIZED,                    // Represents the init state of log replication FSM
    IN_SNAPSHOT_SYNC,               // Represents the state in which snapshot sync (full-sync) is being performed
    IN_LOG_ENTRY_SYNC,              // Represents the state in which log entry sync (delta-sync) is being performed
    IN_REQUIRE_SNAPSHOT_SYNC,       // Represents the state in which a snapshot sync is required due to a cancel or
                                    // error (e.g., Trim Exception)
    STOPPED                         // Represents the state where the FSM is stopped.
}
