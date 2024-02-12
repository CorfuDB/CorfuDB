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
     * Represents the ID of snapshot_sync or log_entry_sync.
     *
     * This is used to correlate the sync ID and the FSM event, and if an FSM event is received for some other sync,
     * it is effectively ignored.
     */
    private UUID syncId;

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
     * Constructor
     *
     * @param syncId identifier of the request that preceded this event.
     */
    public LogReplicationEventMetadata(UUID syncId) {
        this.syncId = syncId;
    }

    /**
     * Constructor
     *
     * @param syncId identifier of the request that preceded this event.
     */
    public LogReplicationEventMetadata(UUID syncId, boolean forceSnapshotSync) {
        this.syncId = syncId;
        this.forceSnapshotSync = forceSnapshotSync;
    }

    /**
     * Constructor
     *
     * @param syncId identifier of the request that preceded this event.
     * @param syncTimestamp last synced timestamp.
     */
    public LogReplicationEventMetadata(UUID syncId, long syncTimestamp) {
        this.syncId = syncId;
        this.lastLogEntrySyncedTimestamp = syncTimestamp;
    }

    /**
     * Constructor
     *
     * @param syncId identifier of the request that preceded this event.
     * @param syncTimestamp last synced timestamp.
     * @param baseSnapshot last base snapshot
     */
    public LogReplicationEventMetadata(UUID syncId, long syncTimestamp, long baseSnapshot) {
        this(syncId, syncTimestamp);
        this.lastTransferredBaseSnapshot = baseSnapshot;
    }

    /**
     * Constructor
     *
     * @param syncId identifier of the request that preceded this event.
     * @param syncTimestamp last synced timestamp.
     * @param baseSnapshot last base snapshot
     */
    public LogReplicationEventMetadata(UUID syncId, long syncTimestamp, long baseSnapshot, boolean forcedSnapshotSync) {
        this(syncId, syncTimestamp);
        this.lastTransferredBaseSnapshot = baseSnapshot;
        this.forceSnapshotSync = forcedSnapshotSync;
    }

    public UUID getSyncId() {
        return this.syncId;
    }

    public long getLastLogEntrySyncedTimestamp() {
        return this.lastLogEntrySyncedTimestamp;
    }

    public long getLastTransferredBaseSnapshot() {
        return this.lastTransferredBaseSnapshot;
    }

    public boolean isForcedSnapshotSync() { return this.forceSnapshotSync; }
}

