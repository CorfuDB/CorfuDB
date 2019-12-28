package org.corfudb.infrastructure;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.Address;

import java.nio.BufferOverflowException;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sequencer server cache.
 * Contains transaction conflict-resolution data structures.
 * <p>
 * The SequencerServer use its own thread/s. To guarantee correct tx conflict-resolution,
 * the {@link SequencerServerCache#cacheHashtable} must be updated
 * along with {@link SequencerServerCache#maxConflictWildcard} at the same time (atomically) to prevent race condition
 * when the conflict stream is already evicted from the cache but `maxConflictWildcard` is not updated yet,
 * which can cause situation when sequencer let the transaction go but the tx has to be cancelled.
 * <p>
 * SequencerServerCache achieves consistency by using single threaded cache. It's done by following code:
 * `.executor(Runnable::run)`
 */
@Slf4j
public class SequencerServerCache {
    /**
     * TX conflict-resolution information:
     * <p>
     * a cache of recent conflict keys and their latest global-log position.
     */
    private final Hashtable<ConflictTxStream, Long> cacheHashtable;
    private final PriorityQueue<ConflictTxStreamEntry> cacheEntries; //sorted according to address
    private final int cacheSize;
    private long maxSequencer = Address.NOT_FOUND; //the max sequence in cacheEntries.
    /**
     * A "wildcard" representing the maximal update timestamp of
     * all the conflict keys which were evicted from the cache
     */
    @Getter
    private long maxConflictWildcard = Address.NOT_FOUND;

    /**
     * maxConflictNewSequencer represents the max update timestamp of all the conflict keys
     * which were evicted from the cache by the time this server is elected
     * the primary sequencer. This means that any snapshot timestamp below this
     * actual threshold would abort due to NEW_SEQUENCER cause.
     */
    @Getter
    private long maxConflictNewSequencer = Address.NOT_FOUND;

    /**
     * The cache limited by size.
     * For a synchronous cache we are using a same-thread executor (Runnable::run)
     * https://github.com/ben-manes/caffeine/issues/90
     *
     * @param cacheSize cache size
     */

    public SequencerServerCache(int cacheSize) {
        this.cacheSize = cacheSize;
        cacheHashtable = new Hashtable<> ();
        cacheEntries = new PriorityQueue<> (cacheSize, new ConflictTxStreamEntryComparator());
    }

    /**
     * Returns the value associated with the {@code key} in this cache,
     * or {@code null} if there is no cached value for the {@code key}.
     *
     * @param conflictKey conflict stream
     * @return global address
     */
    public Long getIfPresent(ConflictTxStream conflictKey) {
        return cacheHashtable.get(conflictKey);
    }

    /**
     * Invalidate one record.
     *
     */
    private void invalidateFirst() {
        ConflictTxStreamEntry entry = cacheEntries.poll();
        cacheHashtable.remove(entry.conflictTxStream);
        maxConflictWildcard = Math.max(entry.txVersion, maxConflictWildcard);
        log.trace("Updating maxConflictWildcard. Old = '{}', new ='{}' conflictParam = '{}'.",
                maxConflictWildcard, entry.txVersion, entry.conflictTxStream);
    }

    /**
     * Invalidate all records up to a trim mark.
     *
     * @param trimMark trim mark
     * */
    public void invalidateUpTo(long trimMark) {
        log.debug("Invalidate sequencer cache. Trim mark: {}", trimMark);
        AtomicLong entries = new AtomicLong();
        ConflictTxStreamEntry entry;
        while((entry = cacheEntries.peek())!= null && entry.txVersion >= trimMark) {
            invalidateFirst();
            entries.incrementAndGet();
        }
        log.info("Invalidated entries: {}", entries.get());
    }

    /**
     * The cache size
     *
     * @return cache size
     */
    public long size() {
        return cacheHashtable.size();
    }

    /**
     * Put a value in the cache
     *
     * @param conflictStream conflict stream
     * @param newTail        global tail
     */
    public void put(ConflictTxStream conflictStream, long newTail) {
        if (cacheHashtable.size() == cacheSize) {
            ConflictTxStreamEntry entry = cacheEntries.peek();
            invalidateUpTo(cacheEntries.peek().txVersion);
        }

        ConflictTxStreamEntry entry = new ConflictTxStreamEntry(conflictStream, newTail);
        cacheEntries.add(entry);
        cacheHashtable.put(conflictStream, entry.txVersion);
        maxSequencer = Math.max(newTail, maxSequencer);
    }

    /**
     * Discard all entries in the cache
     */
    public void invalidateAll() {
        cacheHashtable.clear();
        cacheEntries.clear();
        maxConflictWildcard = maxSequencer;
    }

    /**
     * Update max conflict wildcard by a new address
     *
     * @param newMaxConflictWildcard new conflict wildcard
     */
    public void updateMaxConflictAddress(long newMaxConflictWildcard) {
        log.info("updateMaxConflictAddress, new address: {}", newMaxConflictWildcard);
        maxConflictWildcard = newMaxConflictWildcard;
        maxConflictNewSequencer = newMaxConflictWildcard;
        maxSequencer = newMaxConflictWildcard;
    }

    /**
     * Contains the conflict hash code for a stream ID and conflict param.
     */
    @EqualsAndHashCode
    public static class ConflictTxStream {
        private final UUID streamId;
        private final byte[] conflictParam;

        public ConflictTxStream(UUID streamId, byte[] conflictParam) {
            this.streamId = streamId;
            this.conflictParam = conflictParam;
        }

        @Override
        public String toString() {
            return streamId.toString() + conflictParam;
        }
    }

    public static class ConflictTxStreamEntry {
        private final ConflictTxStream conflictTxStream;
        private final long txVersion;

        public ConflictTxStreamEntry(ConflictTxStream stream, long address) {
            conflictTxStream = stream;
            txVersion = address;
        }
    }

    class ConflictTxStreamEntryComparator implements Comparator<ConflictTxStreamEntry> {
        // Overriding compare()method of Comparator
        // for descending order of cgpa
        public int compare(ConflictTxStreamEntry s1, ConflictTxStreamEntry s2) {
            if (s1.txVersion < s2.txVersion)
                return 1;
            else if (s1.txVersion > s2.txVersion)
                return -1;
            return 0;
        }
    }
}
