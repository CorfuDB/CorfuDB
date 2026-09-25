package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import lombok.NonNull;

import java.util.UUID;

/**
 * An Interface for snapshot logreader.
 *
 * A snapshot reader provides the functionality for reading data from Corfu.
 */
public interface SnapshotReader {

    /**
     * Read streams to replicate across sites.
     *
     * @param snapshotRequestId Snapshot Sync request Id
     *
     * @return result of read operation. If the read result is NULL, the snapshot sync will be terminated.
     */
    @NonNull
    SnapshotReadMessage read(UUID snapshotRequestId);

    /**
     * Reset reader in between snapshot syncs.
     *
     * @param snapshotTimestamp new snapshot timestamp.
     */
    void reset(long snapshotTimestamp);

    void setTopologyConfigId(long topologyConfigId);

    /**
     * Sink write budget used to limit coalescing, independently of the source's
     * local write configuration. Zero means unknown. Existing readers may retain
     * their batching policy; an indivisible entry must still fit the sink write.
     */
    default void setSnapshotBatchSizeHint(int maxWriteSize) {
        // Optional hint: custom readers retain their existing batching policy by default.
    }
}
