package org.corfudb.logreplication.send;

import org.corfudb.logreplication.message.DataMessage;

/**
 * An Interface for Log Entry Reader
 *
 * A log entry reader provides the functionality for reading incremental updates from Corfu.
 */
public interface LogEntryReader {

    DataMessage read();

    void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp);
}
