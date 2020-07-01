package org.corfudb.infrastructure.logreplication.replication.send.logreader;


import lombok.NonNull;

import java.util.UUID;

/**
 * An Interface for snapshot logreader.
 *
 * A snapshot logreader provides the functionality for reading data from Corfu.
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
     * Reset logreader in between snapshot syncs.
     *
     * @param snapshotTimestamp new snapshot timestamp.
     */
    void reset(long snapshotTimestamp);

    void setTopologyConfigId(long topologyConfigId);
}
