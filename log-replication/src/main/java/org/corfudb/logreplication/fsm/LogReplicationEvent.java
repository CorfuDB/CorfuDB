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
        SNAPSHOT_SYNC_REQUEST,       // Signal to start a snapshot sync (full-sync)
        TRIMMED_EXCEPTION,          // Log has been trimmed on access
        SNAPSHOT_SYNC_CANCEL,       // Request to cancel snapshot sync
        LEADERSHIP_LOST,            // Leadership has been lost by current node
        START_LOG_ENTRY_SYNC,       // Request incremental sync (delta-sync)
        LOG_REPLICATION_STOP        // Request to stop the FSM and go to StoppedState
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