package org.corfudb.common.metrics.micrometer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Optional;

/**
 * Since guava's cache metrics maintain running totals of different stats (i.e., they always increase) this class
 * registers one gauge per metric that gets restarted every time its polled/reported thus only reporting the cache
 * stats only within the reporting duration.
 */


public class CacheMetrics {

    @Data
    @AllArgsConstructor
    private static class CacheStatsContainer {
        CacheStats snapshot;
    }

    public static void register(Optional<MeterRegistry> metricsRegistry, Cache<?, ?> cache, String cacheName) {

        if (metricsRegistry.isPresent()) {

            Gauge.builder(cacheName, new CacheStatsContainer(cache.stats()), data -> {
                CacheStats currentSnapshot = cache.stats();
                data.snapshot = currentSnapshot.minus(data.snapshot);
                return data.snapshot.hitRate();
            }).tags("result", "hitRate")
                    .strongReference(true)
                    .register(metricsRegistry.get());

            Gauge.builder(cacheName, new CacheStatsContainer(cache.stats()), data -> {
                CacheStats currentSnapshot = cache.stats();
                data.snapshot = currentSnapshot.minus(data.snapshot);
                return data.snapshot.requestCount();
            }).tags("result", "requestCount")
                    .strongReference(true)
                    .register(metricsRegistry.get());

            Gauge.builder(cacheName, new CacheStatsContainer(cache.stats()), data -> {
                CacheStats currentSnapshot = cache.stats();
                data.snapshot = currentSnapshot.minus(data.snapshot);
                return data.snapshot.evictionCount();
            }).tags("result", "evictionCount")
                    .strongReference(true)
                    .register(metricsRegistry.get());

            Gauge.builder(cacheName, new CacheStatsContainer(cache.stats()), data -> {
                CacheStats currentSnapshot = cache.stats();
                data.snapshot = currentSnapshot.minus(data.snapshot);
                return data.snapshot.averageLoadPenalty();
            }).tags("result", "averageLoadPenalty")
                    .strongReference(true)
                    .register(metricsRegistry.get());
        }

    }
}
