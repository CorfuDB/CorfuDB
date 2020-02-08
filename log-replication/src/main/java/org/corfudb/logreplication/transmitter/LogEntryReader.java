package org.corfudb.logreplication.transmitter;

/**
 * An Interface for Log Entry Reader
 *
 * A log entry reader provides the functionality for reading incremental updates from Corfu.
 */
public interface LogEntryReader {

    DataMessage read();
}
