package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sequencer server cache.
 * Contains transaction conflict-resolution data structures.
 * <p>
 * The SequencerServer use its own thread/s. To guarantee correct tx conflict-resolution,
 * the {@link SequencerServerCache#conflictCache} must be updated
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
    private final Cache<ConflictTxStream, Long> conflictCache;

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
    public SequencerServerCache(long cacheSize, CacheWriter<ConflictTxStream, Long> writer) {
        this.conflictCache = Caffeine.newBuilder()
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
        this.conflictCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                //Performing periodic maintenance using current thread (synchronously)
                .executor(Runnable::run)
                .writer(getDefaultCacheWriter())
                .recordStats()
                .build();
    }

    private CacheWriter<ConflictTxStream, Long> getDefaultCacheWriter() {
        return new CacheWriter<ConflictTxStream, Long>() {
            /**
             * Don't do any additional actions during the write operation. Let caffeine write the record into the cache
             * @param key a key
             * @param value a value
             */
            @Override
            public void write(@Nonnull ConflictTxStream key, @Nonnull Long value) {
                //ignore
            }

            /**
             * Eviction policy https://github.com/ben-manes/caffeine/wiki/Writer
             * @param key  conflict stream
             * @param globalAddress global address
             * @param cause a removal cause
             */
            @Override
            public void delete(@Nonnull ConflictTxStream key, Long globalAddress, @Nonnull RemovalCause cause) {
                if (cause == RemovalCause.REPLACED) {
                    String errMsg = String.format("Override error. Conflict key: %s, address: %s", key, globalAddress);
                    throw new IllegalStateException(errMsg);
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
    }

    /**
     * Returns the value associated with the {@code key} in this cache,
     * or {@code null} if there is no cached value for the {@code key}.
     *
     * @param conflictKey conflict stream
     * @return global address
     */
    public Long getIfPresent(ConflictTxStream conflictKey) {
        return conflictCache.getIfPresent(conflictKey);
    }

    /**
     * Invalidate all records up to a trim mark.
     *
     * @param trimMark trim mark
     */
    public void invalidateUpTo(long trimMark) {
        log.debug("Invalidate sequencer cache. Trim mark: {}", trimMark);

        AtomicLong entries = new AtomicLong();

        conflictCache.asMap().forEach((key, txVersion) -> {
            if (txVersion == null || txVersion >= trimMark) {
                return;
            }

            conflictCache.invalidate(key);
            entries.incrementAndGet();
        });

        log.info("Invalidated entries: {}", entries.get());
    }

    /**
     * The cache size
     *
     * @return cache size
     */
    public long size() {
        return conflictCache.estimatedSize();
    }

    /**
     * Put a value in the cache
     *
     * @param conflictStream conflict stream
     * @param newTail        global tail
     */
    public void put(ConflictTxStream conflictStream, long newTail) {
        conflictCache.put(conflictStream, newTail);
    }

    /**
     * Discard all entries in the cache
     */
    public void invalidateAll() {
        log.info("Invalidate sequencer server cache");
        conflictCache.invalidateAll();
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
}
