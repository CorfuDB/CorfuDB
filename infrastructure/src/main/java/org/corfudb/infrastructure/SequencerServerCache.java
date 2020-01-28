package org.corfudb.infrastructure;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.MetricsUtils;

import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.UUID;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Sequencer server cache.
 * Contains transaction conflict-resolution data structures.
 *
 * The cache map maps conflict keys (stream id + key) to versions (long), illustrated below:
 * Conflict Key | ck1 | ck2 | ck3 | ck4
 * Version | v1 | v1 | v2 | v3
 * Consider the case where we need to insert a new conflict key (ck), but the cache is full,
 * we need to evict the oldest conflict keys. In the example above, we can't just evict ck1,
 * we need to evict all keys that map to v1, so we need to evict ck1 and ck2,
 * this eviction policy is FIFO on the version number. The simple FIFO approach in Caffein etc doesn't work here,
 * as it may evict ck1, but not ck2. Notice that we also can't evict ck3 before the keys for v1,
 * that's because it will create holes in the resolution window and can lead to incorrect resolutions.
 *
 * We use priority queue as a sliding window on the versions, where a version can map to multiple keys,
 * so we also need to maintain the beginning of the window which is the maxConflictWildcard variable.
 *
 * SequencerServerCache achieves consistency by using single threaded cache. It's done by following code:
 * `.executor(Runnable::run)`
 */
@NotThreadSafe
@Slf4j
public class SequencerServerCache {
    /**
     * TX conflict-resolution information:
     * a cache of recent conflict keys and their latest global-log position.
     */

    // As the sequencer cache is used by a single thread, it is safe to use hashmap.
    private final HashMap<ConflictTxStream, Long> cacheConflictKeys;
    private final PriorityQueue<ConflictTxStream> cacheEntries; //sorted according to address

    @Getter
    private final int cacheSize; // the max number of entries in SequencerServerCache

    /**
     * A "wildcard" representing the maximal update timestamp of
     * all the conflict keys which were evicted from the cache
     */
    @Getter
    private long maxConflictWildcard;

    /**
     * maxConflictNewSequencer represents the max update timestamp of all the conflict keys
     * which were evicted from the cache by the time this server is elected
     * the primary sequencer. This means that any snapshot timestamp below this
     * actual threshold would abort due to NEW_SEQUENCER cause.
     */
    @Getter
    private long maxConflictNewSequencer;

    /**
     * It is used to calculate the size of ServerCache. Each entry relates two pointers
     * used by HashMap, one pointer in PriorityQueue.
     */
    static final private int ENTRY_OVERHEAD = 24;

    //As calculating object size is expensive, used the value calculated by deepSize
    static final private int CONFLICTTXSTREAM_OBJ_SIZE = 80; //by calculated by deepSize

    /**
     * The cache limited by size.
     * For a synchronous cache we are using a same-thread executor (Runnable::run)
     * https://github.com/ben-manes/caffeine/issues/90
     *
     * @param cacheSize cache size
     */

    public SequencerServerCache(int cacheSize, long maxConflictNewSequencer) {
        this.cacheSize = cacheSize;
        cacheConflictKeys = new HashMap();
        cacheEntries = new PriorityQueue(cacheSize, Comparator.comparingLong
                (a -> ((ConflictTxStream)a).txVersion));
        maxConflictWildcard = maxConflictNewSequencer;
        this.maxConflictNewSequencer = maxConflictNewSequencer;
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
        if (cacheEntries.isEmpty()) {
            return Address.NOT_FOUND;
        }
        return cacheEntries.peek().txVersion;
    }

    /**
     * Invalidate the records with the minAddress. It could be one or multiple records
     * @return the number of entries has been invalidated and removed from the cache.
     */
    private int invalidateFirst() {
        ConflictTxStream firstEntry = cacheEntries.peek();
        if (cacheEntries.size() == 0) {
            return 0;
        }

        int numEntries = 0;
        while (firstAddress() == firstEntry.txVersion) {
            ConflictTxStream entry = cacheEntries.poll();
            cacheConflictKeys.remove(entry);
            numEntries++;
        }

        log.trace("Evict {} entries", numEntries);
        maxConflictWildcard = Math.max(maxConflictWildcard, firstEntry.txVersion);
        return numEntries;
    }

    /**
     * Invalidate all records up to a trim mark (not included).
     *
     * @param trimMark trim mark
     */
    public void invalidateSmallestTxVersion(long trimMark) {
        log.debug("Invalidate sequencer cache. Trim mark: {}", trimMark);
        int entries = 0;
        int pqEntries = 0;
        while (Address.isAddress(firstAddress()) && firstAddress() < trimMark) {
            pqEntries += invalidateFirst();
            entries++;
        }
        log.info("Invalidated entries {} addresses {}", pqEntries, entries);
    }

    /**
     * The cache size as the number of entries
     *
     * @return cache size
     */
    public int size() {
        return cacheConflictKeys.size();
    }

    /**
     * The memory space used by the entries and also the space used by
     * priority queue and hashmap to store the pointers
     * @return the memory space used in bytes:
     */
    public long byteSize() {
        log.debug("the cache has {} entries,  the object size used {}, calculated by beepSize {}",
                size(), CONFLICTTXSTREAM_OBJ_SIZE,
                cacheEntries.isEmpty() ? 0 : MetricsUtils.sizeOf.deepSizeOf(cacheEntries.peek()));
        return size()*(ENTRY_OVERHEAD + CONFLICTTXSTREAM_OBJ_SIZE);
    }

    /*
     * Put a value in the cache
     *
     * @param conflictStream conflict stream
     */
    public boolean put(ConflictTxStream conflictStream) {

        Long val = cacheConflictKeys.get(conflictStream);
        if (val != null && val > conflictStream.txVersion) {
            log.error("For key {} the new entry address {} is smaller than the entry " +
                            "address {} in cache. There is a sequencer regression.",
                    conflictStream, conflictStream.txVersion, cacheConflictKeys.get(conflictStream));
            return false;
        }

        if (cacheConflictKeys.size() == cacheSize) {
            invalidateFirst();
        }

        cacheEntries.add(conflictStream);
        cacheConflictKeys.put(conflictStream, conflictStream.txVersion);
        return true;
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
