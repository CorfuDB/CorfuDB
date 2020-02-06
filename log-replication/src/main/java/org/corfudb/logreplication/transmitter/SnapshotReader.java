package org.corfudb.logreplication.transmitter;

/**
 * An Interface for snapshot reader.
 *
 * A snapshot reader provides the functionality for reading data from Corfu.
 */
public interface SnapshotReader {

    SnapshotReadMessage read();

    void reset(long snapshotTimestamp);
}
