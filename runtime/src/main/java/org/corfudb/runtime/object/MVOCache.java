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
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.CorfuRuntime;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


/**
 * MVOCache is the centralized container that holds the reference to
 * all MVOs. Overall, it provides put and get APIs and manages the
 * cache-related properties (LRU) under the hood.
 */
@Slf4j
public class MVOCache<T extends ICorfuSMR<T>> {

    /**
     * A registry to keep track of all opened MVOs.
     */
    @Getter
    private final ConcurrentHashMap<UUID, MultiVersionObject<T>> allMVOs = new ConcurrentHashMap<>();

    /**
     * A collection of strong references to all versioned objects and their state.
     */
    @Getter
    private final Cache<VersionedObjectIdentifier, T> objectCache;

    private static final long DEFAULT_CACHE_EXPIRY_TIME_IN_SECONDS = 300;

    private static final String HIT_RATIO_NAME = "mvo_cache.hit_ratio";
    private static final String SIZE_NAME = "mvo_cache.size";

    public MVOCache(@Nonnull CorfuRuntime corfuRuntime) {
        this.objectCache = CacheBuilder.newBuilder()
                .maximumSize(corfuRuntime.getParameters().getMaxCacheEntries())
                .expireAfterAccess(DEFAULT_CACHE_EXPIRY_TIME_IN_SECONDS, TimeUnit.SECONDS)
                .expireAfterWrite(DEFAULT_CACHE_EXPIRY_TIME_IN_SECONDS, TimeUnit.SECONDS)
                .removalListener(this::handleEviction)
                .recordStats()
                .build();

        MeterRegistryProvider.getInstance()
                .map(registry -> GuavaCacheMetrics.monitor(registry, objectCache, "mvo_cache"));

        MicroMeterUtils.gauge(HIT_RATIO_NAME, objectCache, cache -> cache.stats().hitRate());
        MicroMeterUtils.gauge(SIZE_NAME, objectCache, Cache::size);
    }

    private void handleEviction(RemovalNotification<VersionedObjectIdentifier, T> notification) {
        log.trace("handleEviction: evicting {} cause {}", notification.getKey(), notification.getCause());
    }

    /**
     * Shutdown the MVOCache and perform any necessary cleanup.
     */
    public void shutdown() {
        MicroMeterUtils.removeGaugesWithNoTags(HIT_RATIO_NAME, SIZE_NAME);
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
     * Register an MVO with this MVOCache.
     * @param objectId The stream id of the provided MVO.
     * @param mvo      The actual MVO being registered.
     */
    public void registerMVO(@Nonnull UUID objectId, @Nonnull MultiVersionObject<T> mvo) {
        allMVOs.putIfAbsent(objectId, mvo);
    }

    @VisibleForTesting
    public Set<VersionedObjectIdentifier> keySet() {
        return ImmutableSet.copyOf(objectCache.asMap().keySet());
    }
}
