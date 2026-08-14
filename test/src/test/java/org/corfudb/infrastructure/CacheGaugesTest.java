package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CacheGaugesTest {

    @Test
    public void testCacheGauges() {
        Cache<Long, Long> dataCache = Caffeine.newBuilder()
                .maximumSize(5)
                .recordStats()
                .executor(Runnable::run)
                .build();

        CacheGauges cacheGauges = new CacheGauges(dataCache);

        // Zero requests no access -> 100% hit rate
        assertEquals(CacheGauges.evictionCount.applyAsDouble(cacheGauges), 0.0, .0001);
        assertEquals(CacheGauges.hitRate.applyAsDouble(cacheGauges), 1.0, .0001);
        assertEquals(CacheGauges.hitCount.applyAsDouble(cacheGauges), 0.0, .0001);
        assertEquals(CacheGauges.missCount.applyAsDouble(cacheGauges), 0.0, .0001);

        // One request, one miss -> 0% hit rate
        dataCache.getIfPresent(1L);
        assertEquals(CacheGauges.evictionCount.applyAsDouble(cacheGauges), 0.0, .0001);
        assertEquals(CacheGauges.hitRate.applyAsDouble(cacheGauges), 0.0, .0001);
        assertEquals(CacheGauges.hitCount.applyAsDouble(cacheGauges), 0.0, .0001);
        assertEquals(CacheGauges.missCount.applyAsDouble(cacheGauges), 1.0, .0001);

        // One request, no miss -> 100% hit rate
        dataCache.put(1L, 1L);
        assertEquals(CacheGauges.evictionCount.applyAsDouble(cacheGauges), 0.0, .0001);
        assertEquals(CacheGauges.hitRate.applyAsDouble(cacheGauges), 1.0, .0001);
        assertEquals(CacheGauges.hitCount.applyAsDouble(cacheGauges), 0.0, .0001);
        assertEquals(CacheGauges.missCount.applyAsDouble(cacheGauges), 0.0, .0001);

        // Three requests, one miss -> 66.66% hit rate
        dataCache.getIfPresent(1L);
        dataCache.getIfPresent(1L);
        dataCache.getIfPresent(2L);
        assertEquals(CacheGauges.evictionCount.applyAsDouble(cacheGauges), 0.0, .0001);
        assertEquals(CacheGauges.hitRate.applyAsDouble(cacheGauges), 2.0/3.0, .0001);
        assertEquals(CacheGauges.hitCount.applyAsDouble(cacheGauges), 2.0, .0001);
        assertEquals(CacheGauges.missCount.applyAsDouble(cacheGauges), 1.0, .0001);

        // Five inserts requests, one eviction  -> 100% hit rate
        dataCache.put(2L, 1L);
        dataCache.put(3L, 1L);
        dataCache.put(4L, 1L);
        dataCache.put(5L, 1L);
        dataCache.put(6L, 1L);

        assertEquals(CacheGauges.evictionCount.applyAsDouble(cacheGauges), 1.0, .0001);
        assertEquals(CacheGauges.hitRate.applyAsDouble(cacheGauges), 1.0, .0001);
        assertEquals(CacheGauges.hitCount.applyAsDouble(cacheGauges), 0.0, .0001);
        assertEquals(CacheGauges.missCount.applyAsDouble(cacheGauges), 0.0, .0001);
    }
}
