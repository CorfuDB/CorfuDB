package org.corfudb.logreplication.fsm;

import lombok.Data;

import java.util.UUID;

/**
 * This class represents a Log Replication Event.
 *
 * An event triggers the transition from one state to another.
 */
@Data
public class LogReplicationEvent {

    /**
     * Enum listing the various type of LogReplicationEvent.
     */
    public enum LogReplicationEventType {
        SNAPSHOT_SYNC_REQUEST,      // External event which signals start of a snapshot transmit (full-sync)
        TRIMMED_EXCEPTION,          // Internal event indicating that log has been trimmed on access
        SNAPSHOT_SYNC_CANCEL,       // External event requesting to cancel snapshot transmit (full-sync)
        REPLICATION_START,          // External event which signals start of log replication process
        REPLICATION_STOP,           // External event which signals stop of log replication process
        SNAPSHOT_SYNC_COMPLETE,     // Internal event which signals snapshot sync has been completed
        REPLICATION_SHUTDOWN        // External/Internal event which signals log replication to be terminated
    }

    /*
     *  Log Replication Event Identifier
     */
    private UUID eventID;

    /*
     *  Log Replication Event Type
     */
    private LogReplicationEventType type;

    /**
     * Constructor
     *
     * @param type log replication event type
     */
    public LogReplicationEvent(LogReplicationEventType type) {
        this.eventID = UUID.randomUUID();
        this.type = type;
    }

    /**
     * Constructor
     *
     * @param type log replication event type
     * @param eventID unique identifier of the event
     */
    public LogReplicationEvent(LogReplicationEventType type, UUID eventID) {
        this.eventID = eventID;
        this.type = type;
    }
}