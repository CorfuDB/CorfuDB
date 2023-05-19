package org.corfudb.runtime.object;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.binder.cache.GuavaCacheMetrics;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.LambdaUtils;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

    private ScheduledExecutorService recorder = null;

    private Map<UUID, Integer> evictionCounter = null;

    private final Boolean isCacheEntryMetricsDisabled;

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

        isCacheEntryMetricsDisabled = corfuRuntime.getParameters().isCacheEntryMetricsDisabled();
        if (Boolean.FALSE.equals(isCacheEntryMetricsDisabled)) {
            recorder = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("MVOCache-Recorder")
                            .build()
            );
            recorder.scheduleAtFixedRate(
                    () -> LambdaUtils.runSansThrow(this::runRecorder),
                    30,
                    60,
                    TimeUnit.SECONDS
            );

            evictionCounter = new ConcurrentHashMap<>();
        }
    }

    private void runRecorder() {
        Map<UUID, Long> tableCount = objectCache.asMap().keySet().stream()
                .collect(Collectors.groupingBy(key -> key.getObjectId(), Collectors.counting()));

        // List of table UUIDs and the table's snapshot count,
        // sorted in descending order by snapshot count.
        // Ignore the tables whose count is smaller than 10.
        List<Map.Entry<UUID, Long>> sortedTableCount = tableCount.entrySet()
                .stream().filter(e -> e.getValue() >= 10)
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toList());

        // List of table UUIDs and the table's eviction count in the last record interval,
        // sorted in descending order by eviction count.
        // Ignore the tables whose count is smaller than 10.
        List<Map.Entry<UUID, Integer>> sortedEvictionCount = evictionCounter.entrySet()
                .stream().filter(e -> e.getValue() >= 10)
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toList());
        evictionCounter.clear();
        log.info("MVOCache snapshot counts: {}\nMVOCache eviction counts: {}", sortedTableCount, sortedEvictionCount);
    }

    private void handleEviction(RemovalNotification<VersionedObjectIdentifier, T> notification) {
        log.trace("handleEviction: evicting {} cause {}", notification.getKey(), notification.getCause());

        if (Boolean.FALSE.equals(isCacheEntryMetricsDisabled)) {
            evictionCounter.compute(notification.getKey().getObjectId(),
                    (uuid, currentValue) -> currentValue == null ? 1 : currentValue + 1);
        }
    }

    /**
     * Shutdown the MVOCache and perform any necessary cleanup.
     */
    public void shutdown() {
        if (recorder != null) {
            recorder.shutdownNow();
        }
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
