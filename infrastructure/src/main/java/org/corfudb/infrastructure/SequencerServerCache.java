package org.corfudb.infrastructure;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.jcip.annotations.NotThreadSafe;
import org.corfudb.runtime.view.Address;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sequencer server cache.
 * Contains transaction conflict-resolution data structures.
 * <p>
 * The SequencerServer use its own thread/s. To guarantee correct tx conflict-resolution,
 * the {@link SequencerServerCache#cacheConflictKeys} must be updated
 * along with {@link SequencerServerCache#maxConflictWildcard} at the same time (atomically) to prevent race condition
 * when the conflict stream is already evicted from the cache but `maxConflictWildcard` is not updated yet,
 * which can cause situation when sequencer let the transaction go but the tx has to be cancelled.
 * <p>
 * SequencerServerCache achieves consistency by using single threaded cache. It's done by following code:
 * `.executor(Runnable::run)`
 */
@NotThreadSafe
@Slf4j
public class SequencerServerCache {
    /**
     * TX conflict-resolution information:
     * <p>
     * a cache of recent conflict keys and their latest global-log position.
     */

    // As the sequencer cache is used by a single thread, it is safe to use hashmap.
    private final HashMap<ConflictTxStream, Long> cacheConflictKeys;
    private final PriorityQueue<ConflictTxStream> cacheEntries; //sorted according to address

    private final int cacheSize;
    private long maxAddress = Address.NOT_FOUND; //the max sequence in cacheEntries.
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
        cacheConflictKeys = new HashMap<> ();
        cacheEntries = new PriorityQueue<> (cacheSize, Comparator.comparingLong((ConflictTxStream a) -> a.txVersion));
    }

    /**
     * Returns the value associated with the {@code key} in this cache,
     * or {@code null} if there is no cached value for the {@code key}.
     *
     * @param conflictKey conflict stream
     * @return global address
     */
    public Long getIfPresent(ConflictTxStream conflictKey) {
        return cacheConflictKeys.get(conflictKey);
    }

    /**
     * The first address in the priority queue.
     */
    public long firstAddress() {
        if (cacheEntries.isEmpty())
            return Address.NOT_FOUND;

        return cacheEntries.peek().txVersion;
    }

    /**
     * Invalidate the records with the minAddress. It could be one or multiple records
     */
    private void invalidateFirst() {
        ConflictTxStream firstEntry = cacheEntries.peek();
        if (firstEntry == null) {
            return;
        }

        int i = 0;
        while (!cacheEntries.isEmpty() && cacheEntries.peek().txVersion == firstEntry.txVersion) {
            ConflictTxStream entry = cacheEntries.poll();
            cacheConflictKeys.remove(entry);
            i++;
        }

        log.debug("Evict {} entries", i);
        maxConflictWildcard = Math.max(maxConflictWildcard, firstEntry.txVersion);
    }

    /**
     * Invalidate all records up to a trim mark (not included).
     *
     * @param trimMark trim mark
     */
    public void invalidateUpTo(long trimMark) {
        log.debug("Invalidate sequencer cache. Trim mark: {}", trimMark);
        int entries = 0;

        while(Address.isAddress(firstAddress()) && firstAddress() < trimMark) {
            invalidateFirst();
            entries++;
        }
        log.info("Invalidated entries {}", entries);
    }

    /**
     * The cache size
     *
     * @return cache size
     */
    public int size() {
        return cacheConflictKeys.size();
    }

    /*
     * Put a value in the cache
     *
     * @param conflictStream conflict stream
     */
    public void put(ConflictTxStream conflictStream) {
        if (cacheConflictKeys.size() == cacheSize) {
            invalidateFirst();
        }

        cacheEntries.add(conflictStream);
        cacheConflictKeys.put(conflictStream, conflictStream.txVersion);
        maxAddress = Math.max(conflictStream.txVersion, maxAddress);
    }

    /**
     * Discard all entries in the cache
     */
    public void invalidateAll() {
        log.info("Invalidate all entries in sequencer server cache and update maxConflictWildcard to {}", maxAddress);
        cacheConflictKeys.clear();
        cacheEntries.clear();
        maxConflictWildcard = maxAddress;
    }

    /**
     * Update max conflict wildcard by a new address
     *
     * @param newMaxConflictWildcard new conflict wildcard
     */
    public void updateMaxConflictAddress(long newMaxConflictWildcard) {
        log.info("updateMaxConflictAddress, new address: {}", newMaxConflictWildcard);
        invalidateUpTo(newMaxConflictWildcard);
        maxConflictWildcard = newMaxConflictWildcard;
        maxConflictNewSequencer = newMaxConflictWildcard;
    }

    /**
     * Contains the conflict hash code for a stream ID and conflict param.
     */
    @EqualsAndHashCode
    public static class ConflictTxStream {

        @Getter
        private final UUID streamId;
        @Getter
        private final byte[] conflictParam;

        @EqualsAndHashCode.Exclude
        public final long txVersion;

        public ConflictTxStream(UUID streamId, byte[] conflictParam, long address) {
            this.streamId = streamId;
            this.conflictParam = conflictParam;
            txVersion = address;
        }

        @Override
        public String toString() {
            return streamId.toString() + conflictParam;
        }
    }
}
