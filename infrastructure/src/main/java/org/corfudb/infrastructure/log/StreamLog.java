package org.corfudb.infrastructure.log;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.OverwriteCause;

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
     * Prefix trim the global log.
     * @param address address to trim the log up to
     */
    void prefixTrim(long address);

    /**
     * Remove all trimmed addresses from the StreamLog.
     */
    void compact();

    /**
     * Get the global tail and stream tails.
     */
    TailsResponse getTails(List<UUID> streams);

    /**
     * Get the global log tail.
     */
    long getLogTail();

    /**
     * Get global and all stream tails.
     */
    TailsResponse getAllTails();

    /**
     * Get the address space for every stream.
     */
    StreamsAddressResponse getStreamsAddressSpace();

    /**
     * Get the first untrimmed address in the address space.
     */
    long getTrimMark();

    /**
     * Returns the known addresses in this Log Unit in the specified consecutive
     * range of addresses.
     *
     * @param rangeStart Start address of range.
     * @param rangeEnd   End address of range.
     * @return Set of known addresses.
     */
    Set<Long> getKnownAddressesInRange(long rangeStart, long rangeEnd);

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
     * Clears all data and resets all segment handlers.
     */
    void reset();

    /**
     * Get overwrite cause for a given address.
     *
     * @param address global log address
     * @param entryToWrite entry which would cause the overwrite
     * @return (OverwriteCause) Cause of the overwrite
     */
    default OverwriteCause getOverwriteCauseForAddress(long address, LogData entryToWrite) {
        if (entryToWrite == null) {
            throw new IllegalArgumentException("Entry for " + address + " doesn't exist");
        }

        LogData localEntry = read(address);

        if (localEntry == null) {
            throw new IllegalStateException("Detected overwrite on " + address + " but couldn't find the entry locally");
        }

        if (localEntry.isTrimmed()) {
            return OverwriteCause.TRIM;
        } else if (localEntry.isHole()) {
            return OverwriteCause.HOLE;
        } else if (localEntry.equals(entryToWrite) && Arrays.equals(localEntry.getData(), entryToWrite.getData())) {
            return OverwriteCause.SAME_DATA;
        } else {
            return OverwriteCause.DIFF_DATA;
        }
    }
}
