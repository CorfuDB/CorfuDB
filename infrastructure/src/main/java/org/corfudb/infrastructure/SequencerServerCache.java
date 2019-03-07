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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
                //Performing periodic maintenance using current thread (synchronously)
                .executor(Runnable::run)
                .recordStats()
                .build();
    }

    /**
     * The cache limited by size.
     * For a synchronous cache we are using a same-thread executor (Runnable::run)
     * https://github.com/ben-manes/caffeine/issues/90
     *
     * @param cacheSize cache size
     */
    public SequencerServerCache(long cacheSize) {
        CacheWriter<String, Long> writer = new CacheWriter<String, Long>() {
            @Override
            public void write(@Nonnull String key, @Nonnull Long value) {
                //ignore
            }

            /**
             * Eviction policy https://github.com/ben-manes/caffeine/wiki/Writer
             * @param key stream id
             * @param globalAddress global address
             * @param cause a removal cause
             */
            @Override
            public void delete(@Nonnull String key, @Nullable Long globalAddress, @Nonnull RemovalCause cause) {
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
            }
        };

        this.conflictToGlobalTailCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                //Performing periodic maintenance using current thread (synchronously)
                .executor(Runnable::run)
                .writer(writer)
                .recordStats()
                .build();
    }

    public Long getIfPresent(String conflictKeyHash) {
        return conflictToGlobalTailCache.getIfPresent(conflictKeyHash);
    }

    /**
     * Invalidate all records up to a trim mark.
     * @param trimMark trim mark
     */
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
        return conflictToGlobalTailCache.estimatedSize();
    }

    public void put(String conflictHashCode, long newTail) {
        conflictToGlobalTailCache.put(conflictHashCode, newTail);
    }

    public void invalidateAll() {
        log.info("Invalidate sequencer server cache");
        conflictToGlobalTailCache.invalidateAll();
    }

    public void updateMaxConflictAddress(long newMaxConflictWildcard) {
        log.info("updateMaxConflictAddress, new address: {}", newMaxConflictWildcard);
        maxConflictWildcard = newMaxConflictWildcard;
        maxConflictNewSequencer = newMaxConflictWildcard;
    }
}
