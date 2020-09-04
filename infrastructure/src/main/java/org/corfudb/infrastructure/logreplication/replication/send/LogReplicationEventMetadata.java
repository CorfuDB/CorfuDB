package org.corfudb.infrastructure.logreplication.replication.send;

import lombok.Getter;

import java.util.UUID;

/**
 * Los Replication Event Metadata
 */
public class LogReplicationEventMetadata {

    @Getter
    private static final UUID NIL_UUID = new UUID(0,0);

    /*
     * Represents the request/event Id that preceded this event.
     *
     * This is used to correlate the event with the state in which it was originated.
     * For example, a trimmed exception from state A vs. a trimmed exception from state B.
     */
    private UUID requestId;

    /*
     * Represents the last log entry synced timestamp.
     */
    private long lastLogEntrySyncedTimestamp;

    /*
     * Represents the last base snapshot timestamp.
     */
    private long lastTransferredBaseSnapshot;

    /**
     * Empty Metadata
     *
     * @return an empty instance of log replication event metadata
     */
    public static LogReplicationEventMetadata empty() {
        return new LogReplicationEventMetadata(NIL_UUID, -1L);
    }

    /**
     * Constructor
     *
     * @param requestId identifier of the request that preceded this event.
     */
    public LogReplicationEventMetadata(UUID requestId) {
        this.requestId = requestId;
    }

    /**
     * Constructor
     *
     * @param requestId identifier of the request that preceded this event.
     * @param syncTimestamp last synced timestamp.
     */
    public LogReplicationEventMetadata(UUID requestId, long syncTimestamp) {
        this.requestId = requestId;
        this.lastLogEntrySyncedTimestamp = syncTimestamp;
    }

    /**
     * Constructor
     *
     * @param requestId identifier of the request that preceded this event.
     * @param syncTimestamp last synced timestamp.
     * @param baseSnapshot last base snapshot
     */
    public LogReplicationEventMetadata(UUID requestId, long syncTimestamp, long baseSnapshot) {
        this(requestId, syncTimestamp);
        this.lastTransferredBaseSnapshot = baseSnapshot;
    }

    public UUID getRequestId() {
        return this.requestId;
    }

    public long getLastLogEntrySyncedTimestamp() {
        return this.lastLogEntrySyncedTimestamp;
    }

    public long getLastTransferredBaseSnapshot() {
        return this.lastTransferredBaseSnapshot;
    }
}

