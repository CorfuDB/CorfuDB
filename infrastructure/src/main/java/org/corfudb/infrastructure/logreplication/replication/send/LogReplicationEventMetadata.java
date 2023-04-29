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
     * Represents the request ID of snapshot_sync or log_entry_sync state
     *
     * This is used to correlate the sync and the event.
     * For example, a snapshot sync ID 1 , event E1 and a snapshot sync ID2 with event E2
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

    private boolean forceSnapshotSync = false;

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
     */
    public LogReplicationEventMetadata(UUID requestId, boolean forceSnapshotSync) {
        this.requestId = requestId;
        this.forceSnapshotSync = forceSnapshotSync;
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

    /**
     * Constructor
     *
     * @param forceSnapshotSync true, if snapshot sync has been forced by caller.
     *                          false, otherwise.
     */
    public LogReplicationEventMetadata(boolean forceSnapshotSync) {
        this(NIL_UUID, -1L);
        this.forceSnapshotSync = forceSnapshotSync;
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

    public boolean isForcedSnapshotSync() { return this.forceSnapshotSync; }
}

