package org.corfudb.infrastructure.log;

import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * An interface definition that specifies an api to interact with a StreamLog.
 *
 * <p>Created by maithem on 7/15/16.
 */

public interface StreamLog {

    /**
     * Append an entry to the stream log.
     *
     * @param address address of append entry
     * @param entry   entry to append to the log
     */
    void append(long address, LogData entry);

    /**
     * Append a range of consecutive entries ordered by their addresses.
     * Entries that are trimmed, or overwrites other addresses are ignored
     * (i.e. they are not written) and an OverwriteException is not thrown.
     *
     * @param entries entries to append to the log
     */
    void append(List<LogData> entries);

    /**
     * Given an address, read the corresponding stream log entry.
     *
     * @param address address to read from the log
     * @return stream entry if it exists, otherwise return null
     */
    LogData read(long address);

    /**
     * Inspect if the stream log contains the entry at given address.
     *
     * @param address the address to inspect
     * @return true if stream log contains the entry at the address, false otherwise
     */
    boolean contains(long address);

    /**
     * Given an address, read the corresponding garbage log entry.
     *
     * @param address address to read from the log
     * @return garbage entry if it exists, otherwise return null
     */
    LogData readGarbageEntry(long address);

    /**
     * Return the current compaction marks for a list of stream
     *
     * @return a map of stream to its compaction mark.
     */
    long getGlobalCompactionMark();

    /**
     * Start running log compactor.
     */
    void startCompactor();

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
     * Get the committed log tail.
     */
    long getCommittedTail();

    /**
     * Update the committed log tail.
     */
    void updateCommittedTail(long committedTail);

    /**
     * Get the address space for every stream.
     */
    StreamsAddressResponse getStreamsAddressSpace();

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
     * @param oldEntry the existing entry
     * @param newEntry entry which would cause the overwrite
     * @return cause of the overwrite
     */
    static OverwriteCause getOverwriteCauseForAddress(LogData oldEntry, LogData newEntry) {
        OverwriteCause cause = OverwriteCause.DIFF_DATA;

        if (oldEntry != null) {
            if (oldEntry.isHole()) {
                cause = OverwriteCause.HOLE;
            } else if (newEntry.getData() != null && oldEntry.getData() != null &&
                    oldEntry.getData().length == newEntry.getData().length) {
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
     * Check whether the data can be appended to a given log address.
     * Note that it is not permitted multiple threads to access the same log address
     * concurrently through this method, this method does not lock or synchronize.
     *
     * @param address  the log address of append
     * @param oldEntry the existing entry
     * @param newEntry the log entry to append
     * @throws DataOutrankedException if the log entry cannot be assigned to this log address
     *                                as there is a data with higher rank
     * @throws ValueAdoptedException  if the new message is a proposal during the two phase recovery
     *                                write and there is an existing
     *                                data at this log address already.
     * @throws OverwriteException     if the new data is with rank 0 (not from recovery write).
     *                                This can happen only if there is a bug in the client implementation.
     */
    static void assertAppendPermittedUnsafe(long address, LogData oldEntry, LogData newEntry)
            throws DataOutrankedException, ValueAdoptedException {
        if (oldEntry.getType() == DataType.EMPTY) {
            return;
        }
        if (newEntry.getRank().getRank() == 0) {
            // data consistency in danger
            throw new OverwriteException(OverwriteCause.DIFF_DATA);
        }

        int compare = newEntry.getRank().compareTo(oldEntry.getRank());

        if (compare < 0) {
            throw new DataOutrankedException();
        }
        if (compare > 0) {
            if (newEntry.getType() == DataType.RANK_ONLY
                    && oldEntry.getType() != DataType.RANK_ONLY) {
                // the new data is a proposal, the other data is not,
                // so the old value should be adopted
                ReadResponse resp = new ReadResponse();
                resp.put(address, oldEntry);
                throw new ValueAdoptedException(resp);
            }
        }
    }
}
