package org.corfudb.runtime.object;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableSet;
import io.micrometer.core.instrument.binder.cache.GuavaCacheMetrics;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.PersistedCorfuTable;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;


/**
 * MVOCache is the centralized container that holds the reference to
 * all MVOs. Overall, it provides put and get APIs and manages the
 * cache-related properties (LRU) under the hood.
 */
@Slf4j
public class MVOCache<S extends SnapshotGenerator<S>> {

    /**
     * A collection of strong references to all versioned objects and their state.
     */
    @Getter
    final Cache<VersionedObjectIdentifier, SMRSnapshot<S>> objectCache;

    /**
     * Construct an MVO cache whose eviction policy is strictly time based.
     * This is used in the context of {@link PersistedCorfuTable} where
     * snapshots are merely references and thus do not consume any significant
     * amount of resources.
     *
     * @param expireAfter time after which a snapshot will be considered invalid
     */
    public MVOCache(Duration expireAfter) {
        // See https://github.com/google/guava/wiki/CachesExplained#when-does-cleanup-happen
        this.objectCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expireAfter)
                .removalListener(this::handleEviction)
                .recordStats()
                .build();

        MeterRegistryProvider.getInstance()
                .map(registry -> GuavaCacheMetrics.monitor(registry, objectCache, "mvo_cache"));
    }

    public MVOCache(@Nonnull CorfuRuntime corfuRuntime) {
        // If not explicitly set by user, it takes default value in CorfuRuntimeParameters
        long maxCacheSize = corfuRuntime.getParameters().getMaxMvoCacheEntries();
        if (corfuRuntime.getParameters().isCacheDisabled()) {
            // Do not allocate memory when cache is disabled.
            maxCacheSize = 0;
        }

        CacheBuilder<VersionedObjectIdentifier, SMRSnapshot<S>> mvoCacheBuilder = CacheBuilder.newBuilder()
                .maximumSize(maxCacheSize)
                .removalListener(this::handleEviction)
                .recordStats();

        Duration expireAfterWrite = corfuRuntime.getParameters().getMvoCacheExpiryInMemory();
        if (expireAfterWrite.toMillis() > 0) {
            // IMPORTANT: When this CorfuRuntime parameter is set to 0 -> no time-based eviction.
            // But we can't call mvoCacheBuilder.expireAfterWrite(0), because that has different effect:
            // From Guava Cache doc: When duration is zero, this method hands off to maximumSize(0)
            mvoCacheBuilder.expireAfterWrite(expireAfterWrite);
        }

        log.info("MVO cache parameters: size={} expireAfterWrite={}", maxCacheSize, expireAfterWrite);
        this.objectCache = mvoCacheBuilder.build();

        MeterRegistryProvider.getInstance()
                .map(registry -> GuavaCacheMetrics.monitor(registry, objectCache, "mvo_cache"));
    }

    public void handleEviction(RemovalNotification<VersionedObjectIdentifier, SMRSnapshot<S>> notification) {
        notification.getValue().getMetrics().setEvictedTs(System.nanoTime());
        notification.getValue().getMetrics().recordMetrics(notification.getKey().getObjectId().toString());
        if (log.isTraceEnabled()) {
            log.trace("handleEviction: [{}] {}, Cause: {}", notification.getKey(),
                    notification.getValue().getMetrics(), notification.getCause());
        }
        notification.getValue().release();
    }

    /**
     * Shutdown the MVOCache and perform any necessary cleanup.
     */
    public void shutdown() {
    }

    /**
     * Retrieve a versioned object from the cache, if present.
     * @param voId The desired object version.
     * @return An optional containing the corresponding versioned object, if present.
     */
    public Optional<SMRSnapshot<S>> get(@Nonnull VersionedObjectIdentifier voId) {
        if (log.isTraceEnabled()) {
            log.trace("MVOCache: performing a get for {}", voId.toString());
        }

        return Optional.ofNullable(objectCache.getIfPresent(voId));
    }

    /**
     * Put a versioned object into the cache.
     * @param voId   The version of the object being placed into the cache.
     * @param object The actual underlying object corresponding to this voId.
     */
    public void put(@Nonnull VersionedObjectIdentifier voId, @Nonnull SMRSnapshot<S> object) {
        objectCache.cleanUp();
        if (log.isTraceEnabled()) {
            log.trace("MVOCache: performing a put for {}", voId);
        }

        object.getMetrics().setCacheEntryTs(System.nanoTime());
        objectCache.put(voId, object);
    }

    /**
     * Invalidate all versioned objects with the given object ID.
     * @param objectId
     */
    public void invalidateAllVersionsOf(@Nonnull UUID objectId) {
        if (log.isTraceEnabled()) {
            log.trace("MVOCache: performing a invalidateAllVersionsOf for {}", objectId);
        }

        List<VersionedObjectIdentifier> voIdsToInvalidate =
                objectCache.asMap().keySet().stream()
                        .filter(voId -> objectId.equals(voId.getObjectId()))
                        .collect(Collectors.toList());
        objectCache.invalidateAll(voIdsToInvalidate);

        // Force cleanup of invalidated and EXPIRED cache entries. Despite still being part of certain
        // internal cache data structures, note that EXPIRED entries do not appear in the asMap() view.
        // Moreover, unlike the invalidateAll() variant of this API, these EXPIRED entries are not
        // removed when invalidateAll(Iterable<? extends Object> keys) is invoked.
        objectCache.cleanUp();
    }

    @VisibleForTesting
    public Set<VersionedObjectIdentifier> keySet() {
        return ImmutableSet.copyOf(objectCache.asMap().keySet());
    }
}
