package org.corfudb.logreplication.fsm;

/**
 * Types of log replication states.
 *
 * Log Replication process can be in one of the following states.
 */
public enum LogReplicationStateType {
    INITIALIZED,
    IN_SNAPSHOT_SYNC,
    IN_LOG_ENTRY_SYNC,
    IN_REQUIRE_SNAPSHOT_SYNC,
    STOPPED
}
