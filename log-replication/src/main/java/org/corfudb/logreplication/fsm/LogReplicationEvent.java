package org.corfudb.logreplication.fsm;

import java.util.UUID;

/**
 * Interface for Log Replication Event.
 */
public interface LogReplicationEvent {

    /**
     * Enum listing the various type of LeaseEvents.
     */
    enum LogReplicationEventType {
        SNAPSHOT_SYNC_REQUEST,      // External event which signals start of a snapshot sync (full-sync)
        TRIMMED_EXCEPTION,          // Internal event indicating that log has been trimmed on access
        SNAPSHOT_SYNC_CANCEL,       // External event requesting to cancel snapshot sync (full-sync)
        REPLICATION_START,          // External event which signals start of log replication process
        REPLICATION_STOP,            // External event which signals stop of log replication process
        SNAPSHOT_SYNC_COMPLETE      // Internal event which signals a snapshot sync has been completed
        // REPLICATION_SHUTDOWN?
    }

    /**
     * Get the Log Replication event id.
     *
     * @return Log Replication event id.
     */
    UUID getId();

    /**
     * Get the event type.
     *
     * @return LogReplicationEventType.
     */
    LogReplicationEventType getType();

}