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
    void append(long address, LogData entry);

    /**
     * Given an address, read the corresponding stream entry.
     * @param address
     * @return Stream entry if it exists, otherwise return null
     */
    LogData read(long address);

    /**
     * Mark a StreamLog address as trimmed.
     * @param address
     */
    void trim(long address);

    /**
     * Prefix trim the global log
     * @param address address to trim the log up to
     */
    void prefixTrim(long address);

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
     * @param address
     */
    void release(long address, LogData entry);
}
