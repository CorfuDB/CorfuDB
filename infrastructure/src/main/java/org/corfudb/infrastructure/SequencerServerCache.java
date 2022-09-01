package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.view.Address;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Sequencer server cache.
 * Contains transaction conflict-resolution data structures.
 * <p>
 * The conflictKeyMap maps conflict keys (stream id + key) to versions (long), illustrated below:
 * Conflict Key | ck1 | ck2 | ck3 | ck4
 * Version | v1 | v1 | v2 | v3
 * Consider the case where we need to insert a new conflict key (ck), but the cache is full.
 * We need to evict the oldest conflict keys. In the example above, we can't just evict ck1,
 * we need to evict all keys that map to v1, so we need to evict ck1 and ck2. This eviction
 * policy is FIFO on the version number. Notice that we also can't evict ck3 before the keys
 * for v1, that's because it will create holes in the resolution window and can lead to
 * incorrect resolutions.
 * <p>
 * We use LinkedHashMap as a sliding window on the versions, since versions are added in strictly increasing
 * order. We also maintain the beginning of the window which is the maxConflictWildcard variable.
 * <p>
 * SequencerServerCache achieves consistency by using single threaded cache.
 */
@NotThreadSafe
@Slf4j
public class SequencerServerCache implements AutoCloseable {

    /**
     * A mapping between conflict keys and their latest global-log position.
     * This map is backed by a LinkedHashMap in order to maintain insertion order.
     * Since the timestamps inserted into the cache are strictly increasing, the sliding
     * window can be maintained within this same data structure.
     */
    @VisibleForTesting
    @Getter(AccessLevel.PUBLIC)
    private final Map<ConflictTxStream, Long> conflictKeyMap;

    /**
     * The max number of entries that the SequencerServerCache may contain.
     */
    @Getter
    private final int capacity;

    /**
     * A "wildcard" representing the maximal update timestamp of
     * all the conflict keys which were evicted from the cache
     */
    @Getter
    private long maxConflictWildcard;

    /**
     * Since the LinkedHashMap does not provide easy access to the largest
     * timestamp, this is maintained explicitly for precondition validation.
     */
    @Getter
    private long maxTimestampInserted = Address.NON_ADDRESS;

    /**
     * maxConflictNewSequencer represents the max update timestamp of all the conflict keys
     * which were evicted from the cache by the time this server is elected
     * the primary sequencer. This means that any snapshot timestamp below this
     * actual threshold would abort due to NEW_SEQUENCER cause.
     */
    @Getter
    private final long maxConflictNewSequencer;

    private static final String CONFLICT_KEYS_COUNTER_NAME = "sequencer.conflict-keys.size";

    /**
     * The Sequencers conflict key cache, limited by size.
     * @param capacity                The max capacity of the cache.
     * @param maxConflictNewSequencer The new max update timestamp of all conflict keys evicted
     *                                from the cache by the time this server is elected as
     *                                primary sequencer.
     */
    public SequencerServerCache(int capacity, long maxConflictNewSequencer) {
        Preconditions.checkArgument(capacity > 0, "sequencer cache capacity must be positive.");

        this.capacity = capacity;
        this.maxConflictWildcard = maxConflictNewSequencer;
        this.maxConflictNewSequencer = maxConflictNewSequencer;

        this.conflictKeyMap = MicroMeterUtils
                .gauge(CONFLICT_KEYS_COUNTER_NAME, new LinkedHashMap<ConflictTxStream, Long>(), LinkedHashMap::size)
                .orElseGet(LinkedHashMap::new);
    }

    /**
     * Returns the global address associated with the conflict key in this cache,
     * or {@code Address.NON_ADDRESS} if there is no such cached address.
     * @param conflictKey The conflict key.
     * @return The global address associated with this hashed conflict key, if present.
     */
    public long get(@NonNull ConflictTxStream conflictKey) {
        return conflictKeyMap.getOrDefault(conflictKey, Address.NON_ADDRESS);
    }

    /**
     * Returns the first (smallest) address in the cache.
     * @return The smallest address currently in the cache.
     */
    @VisibleForTesting
    public long firstAddress() {
        if (conflictKeyMap.isEmpty()) {
            return Address.NOT_FOUND;
        }

        return conflictKeyMap.entrySet().iterator().next().getValue();
    }

    /**
     * Evict the records with the smallest address in the cache.
     * It could be one or multiple records.
     *
     * @return The number of entries that have been evicted from the cache.
     */
    private void evictSmallestTxVersion() {
        evictUpTo(firstAddress() + 1);
    }

    /**
     * Evict all records up to the given address (not included).
     * @param address The given address.
     */
    public void evictUpTo(long address) {
        long numDeletedEntries = 0;
        long smallestAddress;

        Iterator<Map.Entry<ConflictTxStream, Long>> it = conflictKeyMap.entrySet().iterator();

        while (it.hasNext()) {
            smallestAddress = it.next().getValue();
            if (smallestAddress >= address) {
                break;
            }

            it.remove();
            numDeletedEntries++;
            maxConflictWildcard = Math.max(maxConflictWildcard, smallestAddress);
        }

        log.trace("evictUpTo[{}]: entries={}", address, numDeletedEntries);
        MicroMeterUtils.measure(numDeletedEntries, "sequencer.cache.evictions");
    }

    /**
     * Evict records from the cache until the number of records is
     * equal to or less than the cache capacity. Eviction is performed
     * by repeatedly evicting all of the records with the smallest address.
     */
    private void evict() {
        while (conflictKeyMap.size() > capacity) {
            evictSmallestTxVersion();
        }
    }

    /**
     * The cache size as the number of entries.
     * @return The current cache size.
     */
    public int size() {
        return conflictKeyMap.size();
    }

    /**
     * Put the provided conflict keys into the cache, and evict older
     * entries as necessary.
     * @param conflictKeys The conflict keys to add into the cache.
     * @param txVersion    The timestamp associated with these conflict keys.
     */
    public void put(@NonNull List<ConflictTxStream> conflictKeys, long txVersion) {
        // Validate that the conflictKeys can all fit into the cache.
        Preconditions.checkState(conflictKeys.size() <= capacity,
                "too many conflict keys for the capacity=%s of the cache", capacity);

        // Validate that the timestamps are being inserted in strictly increasing order.
        Preconditions.checkState(txVersion > maxTimestampInserted,
                "txVersion=%s is not larger than the previous timestamp=%s inserted",
                txVersion, maxTimestampInserted);

        conflictKeys.forEach(conflictKey -> {
            // This remove is required since inserting a duplicate conflict key into
            // the LinkedHashMap does not update the insertion order.
            this.conflictKeyMap.remove(conflictKey);
            this.conflictKeyMap.put(conflictKey, txVersion);
        });

        maxTimestampInserted = txVersion;

        // If applicable, evict entries until we are below the max cache capacity.
        // Note: this can trigger the eviction of multiple conflict keys from multiple timestamps.
        evict();
    }

    @Override
    public void close() {
        MicroMeterUtils.removeGaugesWithNoTags(CONFLICT_KEYS_COUNTER_NAME);
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

        public ConflictTxStream(UUID streamId, byte[] conflictParam) {
            this.streamId = streamId;
            this.conflictParam = conflictParam;
        }

        @Override
        public String toString() {
            return streamId.toString() + Arrays.toString(conflictParam);
        }
    }
}
