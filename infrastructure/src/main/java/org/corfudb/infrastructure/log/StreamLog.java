package org.corfudb.infrastructure.log;

import java.io.IOException;
import java.util.List;

import org.corfudb.protocols.wireprotocol.LogData;

/**
 * An interface definition that specifies an api to interact with a StreamLog.
 *
 * <p>Created by maithem on 7/15/16.
 */

public interface StreamLog {

    /**
     * Append an entry to the stream log.
     * @param address  address of append entry
     * @param entry    entry to append to the log
     */
    void append(long address, LogData entry);

    /**
     * Append a range of consecutive entries ordered by their addresses.
     * Entries that are trimmed, or overwrites other addresses are ignored
     * (i.e. they are not written) and an OverwriteException is not thrown.
     *
     * @param entries
     */
    void append(List<LogData> entries);

    /**
     * Given an address, read the corresponding stream entry.
     * @param address  address to read from the log
     * @return Stream entry if it exists, otherwise return null
     */
    LogData read(long address);

    /**
     * Mark a StreamLog address as trimmed.
     * @param address  address to trim from the log
     */
    void trim(long address);

    /**
     * Prefix trim the global log.
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
     * Get the first untrimmed address in the address space.
     */
    long getTrimMark();

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
     * unmap/release the memory for entry.
     *
     * @param address  address to release
     */
    void release(long address, LogData entry);

    /**
     * Clears all data and resets all segment handlers.
     */
    void reset();
}
