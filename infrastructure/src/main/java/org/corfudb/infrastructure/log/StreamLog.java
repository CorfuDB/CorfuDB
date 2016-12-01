package org.corfudb.infrastructure.log;

import org.corfudb.protocols.wireprotocol.LogData;

import java.io.IOException;

/**
 * An interface definition that specifies an api to interact with a StreamLog.
 *
 * Created by maithem on 7/15/16.
 */

public interface StreamLog {

    /**
     * Append an entry to the stream log.
     * @param address
     * @param entry
     */
    void append(LogAddress address, LogData entry);

    /**
     * Given an address, read the corresponding stream entry.
     * @param address
     * @return Stream entry if it exists, otherwise return null
     */
    LogData read(LogAddress address);

    /**
     * Sync the stream log file to secondary storage.
     */
    void sync() throws IOException;

    /**
     * Close the stream log.
     */
    void close();
}
