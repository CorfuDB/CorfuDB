package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.Data;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.util.Utils;

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
        LOG_ENTRY_SYNC_REQUEST,     // External event which signals start of log replication process
        REPLICATION_STOP,           // External event which signals stop of log replication process
        SNAPSHOT_SYNC_REQUEST,      // External event which signals start of a snapshot sync (full-sync)
        SNAPSHOT_SYNC_CONTINUE,     // Internal event to continue snapshot sync (broken to accommodate multi-cluster
                                    // replication for a shared thread pool)
        SNAPSHOT_APPLY_IN_PROGRESS, // External event which signals the sender that snapshot apply is still in progress, wait.
        SNAPSHOT_TRANSFER_COMPLETE, // External event which signals the sender that snapshot has been successfully transferred
                                    // and log replication can transition to log entry (incremental) sync
        SNAPSHOT_APPLY_COMPLETE,    // External/Internal event which signals snapshot sync has been completed
        LOG_ENTRY_SYNC_CONTINUE,    // Internal event to continue log entry sync (broken to accommodate multi-cluster
                                    // replication for a shared thread pool)
        SYNC_CANCEL,                // External/Internal event requesting to cancel sync (snapshot or log entry)
        LOG_ENTRY_SYNC_REPLICATED,  // External event which signals a log entry sync has been successfully replicated
                                    // on the standby cluster (ack)
        REPLICATION_SHUTDOWN        // External/Internal event which signals log replication to be terminated (only a
                                    // JVM restart can enable log replication after this)
    }

    /*
     *  Log Replication Event Identifier
     */
    private UUID eventId;

    /*
     *  Log Replication Event Type
     */
    private LogReplicationEventType type;

    /*
     *  Event Metadata
     */
    private LogReplicationEventMetadata metadata;

    /**
     * Constructor
     *
     * @param type log replication event type
     */
    public LogReplicationEvent(LogReplicationEventType type) {
        this(type, LogReplicationEventMetadata.empty());
    }

    /**
     * Constructor used when an event identifier is given in advance.
     * This is used for the case of force snapshot sync for which an
     * identifier was previously computed in order to provide the caller
     * with a tracking identifier.
     *
     * @param type log replication event type
     * @param eventId event unique identifier
     */
    public LogReplicationEvent(LogReplicationEventType type, UUID eventId) {
        this.type = type;
        this.eventId = eventId;
        this.metadata = new LogReplicationEventMetadata(true);
    }

    /**
     * Constructor
     *
     * @param type log replication event type
     * @param metadata log replication event metadata
     *
\     */
    public LogReplicationEvent(LogReplicationEventType type, LogReplicationEventMetadata metadata) {
        this.type = type;
        this.eventId = Utils.genPseudorandomUUID();
        this.metadata = metadata;
    }
}