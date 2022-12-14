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

import javax.annotation.Nonnull;
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
public class MVOCache<T extends ICorfuSMR<T>> {

    /**
     * A collection of strong references to all versioned objects and their state.
     */
    @Getter
    private final Cache<VersionedObjectIdentifier, T> objectCache;

    public MVOCache(@Nonnull CorfuRuntime corfuRuntime) {

        // If not explicitly set by user, it takes default value in CorfuRuntimeParameters
        long maxCacheSize = corfuRuntime.getParameters().getMaxMvoCacheEntries();
        if (corfuRuntime.getParameters().isCacheDisabled()) {
            // Do not allocate memory when cache is disabled.
            maxCacheSize = 0;
        }
        log.info("MVO cache size is set to {}", maxCacheSize);

        this.objectCache = CacheBuilder.newBuilder()
                .maximumSize(maxCacheSize)
                .removalListener(this::handleEviction)
                .recordStats()
                .build();

        MeterRegistryProvider.getInstance()
                .map(registry -> GuavaCacheMetrics.monitor(registry, objectCache, "mvo_cache"));
    }

    private void handleEviction(RemovalNotification<VersionedObjectIdentifier, T> notification) {
        log.trace("handleEviction: evicting {} cause {}", notification.getKey(), notification.getCause());
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
    public Optional<T> get(@Nonnull VersionedObjectIdentifier voId) {
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
    public void put(@Nonnull VersionedObjectIdentifier voId, @Nonnull T object) {
        if (log.isTraceEnabled()) {
            log.trace("MVOCache: performing a put for {}", voId.toString());
        }

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
    }

    @VisibleForTesting
    public Set<VersionedObjectIdentifier> keySet() {
        return ImmutableSet.copyOf(objectCache.asMap().keySet());
    }
}
