package org.corfudb.infrastructure.log;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.TrimmedException;

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
     * Inspect if the stream log contains the entry at given address.
     *
     * @param address the address to inspect
     * @return true if stream log contains the entry at the address, false otherwise
     * @throws TrimmedException if address less than starting address
     */
    boolean contains(long address) throws TrimmedException;

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
     * Get the committed log tail.
     */
    long getCommittedTail();

    /**
     * Update the committed log tail.
     */
    void updateCommittedTail(long committedTail);

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
     * @param entry entry which would cause the overwrite
     * @return (OverwriteCause) Cause of the overwrite
     */
    default OverwriteCause getOverwriteCauseForAddress(long address, LogData entry) {
        LogData currentEntry = read(address);
        OverwriteCause cause = OverwriteCause.DIFF_DATA;

        if (currentEntry != null) {
            if (currentEntry.isHole()) {
                cause = OverwriteCause.HOLE;
            } else if (entry.getData() != null && currentEntry.getData() != null &&
                    currentEntry.getData().length == entry.getData().length) {
                // If the entry is already present and it is not a hole, the write
                // might have been propagated by a fast reader from part of the chain.
                // Compare based on data length. Based on this info client will do an actual
                // verification on the data
                cause = OverwriteCause.SAME_DATA;
            }
        } else {
            // No actual entry is found in this address, there is no apparent cause
            // for the overwrite exception
            cause = OverwriteCause.NONE;
        }
        return cause;
    }

    /**
     * Query if the StreamLog has enough quota to accept writes
     */
    default boolean quotaExceeded() {
        return false;
    }

    /**
     * Query the exact Quota value in bytes
     * @return the quota set in bytes
     */
    default long quotaLimitInBytes() {
        return Long.MAX_VALUE;
    }
}
