package org.corfudb.logreplication.transmit;


import lombok.NonNull;

/**
 * An Interface for snapshot reader.
 *
 * A snapshot reader provides the functionality for reading data from Corfu.
 */
public interface SnapshotReader {

    /**
     * Read streams to replicate across sites.
     *
     * @return result of read operation. If the read result is NULL, the snapshot sync will be terminated.
     */
    @NonNull
    SnapshotReadMessage read();

    /**
     * Reset reader in between snapshot syncs.
     *
     * @param snapshotTimestamp new snapshot timestamp.
     */
    void reset(long snapshotTimestamp);
}
