package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;

import java.util.concurrent.CountDownLatch;
import java.util.function.ToDoubleFunction;

/**
 * This class transforms caffeine's cumulative statistics and reports them as gauges that track stats per poll period.
 */
public class CacheGauges {
    static ToDoubleFunction<CacheGauges> hitRate = f -> {
        f.snapshotIfNecessary();
        f.polledGaugesLatch.countDown();
        return f.current.minus(f.previous).hitRate();
    };

    static ToDoubleFunction<CacheGauges> hitCount = f -> {
        f.snapshotIfNecessary();
        f.polledGaugesLatch.countDown();
        return f.current.minus(f.previous).hitCount();
    };

    static ToDoubleFunction<CacheGauges> missCount = f -> {
        f.snapshotIfNecessary();
        f.polledGaugesLatch.countDown();
        return f.current.minus(f.previous).missCount();
    };

    static ToDoubleFunction<CacheGauges> evictionCount = f -> {
        f.snapshotIfNecessary();
        f.polledGaugesLatch.countDown();
        return f.current.minus(f.previous).evictionCount();
    };
    private volatile CacheStats current;
    private volatile CacheStats previous;
    private volatile CountDownLatch polledGaugesLatch;

    private final Cache cache;

    public CacheGauges(Cache cache) {
        this.polledGaugesLatch = new CountDownLatch(0);
        this.current = CacheStats.empty();
        this.previous = CacheStats.empty();
        this.cache = cache;
    }

    public static void register(Cache cache, String cacheName) {
        CacheGauges cacheGauges = new CacheGauges(cache);
        MicroMeterUtils.gauge(cacheName, cacheGauges, hitRate, "result", "hitRate");
        MicroMeterUtils.gauge(cacheName, cacheGauges, hitCount, "result", "hitCount");
        MicroMeterUtils.gauge(cacheName, cacheGauges, missCount, "result", "missCount");
        MicroMeterUtils.gauge(cacheName, cacheGauges, evictionCount, "result", "evictionCount");
    }

    private void snapshotIfNecessary() {
        if (polledGaugesLatch.getCount() == 0) {
            CacheStats temp = current;
            current = cache.stats();
            previous = temp;
            int totalGauges = 4;
            polledGaugesLatch = new CountDownLatch(totalGauges);
        }
    }
}