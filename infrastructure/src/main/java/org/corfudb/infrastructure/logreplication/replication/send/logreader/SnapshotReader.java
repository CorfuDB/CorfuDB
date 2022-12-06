package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import lombok.NonNull;

import java.util.UUID;

/**
 * Abstract class for snapshot reader. This provides the functionality
 * for reading data from Corfu during snapshot sync.
 */
public abstract class SnapshotReader {

    protected long topologyConfigId;

    /**
     * Read streams to replicate across sites.
     *
     * @param snapshotRequestId Snapshot Sync request Id
     *
     * @return result of read operation. If the read result is NULL, the snapshot sync will be terminated.
     */
    @NonNull
    public abstract SnapshotReadMessage read(UUID snapshotRequestId);

    /**
     * Reset reader in between snapshot syncs.
     *
     * @param snapshotTimestamp new snapshot timestamp.
     */
    public abstract void reset(long snapshotTimestamp);

    public void setTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
    }
}
