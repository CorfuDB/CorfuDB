package org.corfudb.logreplication.transmitter;

/**
 * Interface for a snapshot reader.
 *
 * A snapshot reader provides the functionality for reading data from Corfu.
 */
public interface SnapshotReader {

    SnapshotReadMessage read();

    void reset(long snapshotTimestamp);
}
