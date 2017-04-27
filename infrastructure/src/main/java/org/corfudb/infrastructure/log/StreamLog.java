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
     * @param logAddress
     * @param entry
     */
    void append(LogAddress logAddress, LogData entry);

    /**
     * Given an address, read the corresponding stream entry.
     * @param logAddress
     * @return Stream entry if it exists, otherwise return null
     */
    LogData read(LogAddress logAddress);

    /**
     * Mark a StreamLog address as trimmed.
     * @param logAddress
     */
    void trim(LogAddress logAddress);

    /**
     * Remove all trimmed addresses from the StreamLog.
     */
    void compact();

    /**
     * Get the last global address that was written.
     */
    long getGlobalTail();

    /**
     * Sync the stream log file to secondary storage.
     *
     * @param force force data to secondary storage if true
     */
    void sync(boolean force) throws IOException;

    /**
     * Close the stream log.
     */
    void close();

    /**
     * unmap/release the memory for entry
     *
     * @param logAddress
     */
    void release(LogAddress logAddress, LogData entry);
}
