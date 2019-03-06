package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.Address;

import java.util.concurrent.ConcurrentMap;

/**
 * Sequencer server cache.
 * Contains transaction conflict-resolution data structures.
 */
@Slf4j
public class SequencerServerCache {
    /**
     * TX conflict-resolution information:
     * <p>
     * a cache of recent conflict keys and their latest global-log position.
     */
    private final Cache<String, Long> conflictToGlobalTailCache;

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

    @VisibleForTesting
    public SequencerServerCache(long cacheSize, CacheWriter<String, Long> writer) {
        this.conflictToGlobalTailCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .writer(writer)
                .executor(Runnable::run)
                .recordStats()
                .build();
    }

    public SequencerServerCache(long cacheSize) {
        RemovalListener<String, Long> removalListener = (String key, Long globalAddress, RemovalCause cause) -> {
            if (cause == RemovalCause.REPLACED) {
                return;
            }

            log.trace(
                    "Updating maxConflictWildcard. Old = '{}', new ='{}' conflictParam = '{}'. Cause = '{}'",
                    maxConflictWildcard, globalAddress, key, cause
            );

            if (globalAddress == null) {
                globalAddress = Address.NOT_FOUND;
            }
            maxConflictWildcard = Math.max(globalAddress, maxConflictWildcard);
        };

        this.conflictToGlobalTailCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .executor(Runnable::run)
                .removalListener(removalListener)
                .recordStats()
                .build();
    }

    public Long getIfPresent(String conflictKeyHash) {
        return conflictToGlobalTailCache.getIfPresent(conflictKeyHash);
    }

    public void invalidate(long trimMark) {
        log.debug("Invalidate sequencer cache. Trim mark: {}", trimMark);

        long entries = 0;
        for (String key : conflictToGlobalTailCache.asMap().keySet()) {
            Long currTrimMark = getIfPresent(key);
            if (currTrimMark >= trimMark) {
                continue;
            }

            conflictToGlobalTailCache.invalidate(key);
            entries++;
        }
        log.info("Evicted entries: {}", entries);

    }

    public long size() {
        return asMap().size();
    }

    public void put(String conflictHashCode, long newTail) {
        conflictToGlobalTailCache.put(conflictHashCode, newTail);
    }

    public void invalidate() {
        log.info("Invalidate sequencer server cache");
        conflictToGlobalTailCache.invalidateAll();
    }

    public void updateMaxConflictAddress(long newMaxConflictWildcard) {
        log.info("updateMaxConflictAddress, new address: {}", newMaxConflictWildcard);
        maxConflictWildcard = newMaxConflictWildcard;
        maxConflictNewSequencer = newMaxConflictWildcard;
    }

    public ConcurrentMap<String, Long> asMap() {
        return conflictToGlobalTailCache.asMap();
    }
}
