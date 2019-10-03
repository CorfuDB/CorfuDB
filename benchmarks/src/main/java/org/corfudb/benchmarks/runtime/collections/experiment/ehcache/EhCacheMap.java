package org.corfudb.benchmarks.runtime.collections.experiment.ehcache;

import com.google.common.collect.Streams;
import lombok.NonNull;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * A persistent map backed by EhCache
 *
 * @param <K> key type
 * @param <V> value type
 */
public class EhCacheMap<K, V> implements Map<K, V> {

    private final PersistentCacheManager persistentCacheManager;
    private final ResourcePools resourcePools;
    private final Cache<K, V> cache;
    private final AtomicInteger dataSetSize = new AtomicInteger();

    public EhCacheMap(@NonNull PersistentCacheManager persistentCacheManager,
                      @NonNull ResourcePools resourcePools,
                      @NonNull Class<K> keyType,
                      @NonNull Class<V> valueType) {
        this.persistentCacheManager = persistentCacheManager;
        this.resourcePools = resourcePools;
        CacheConfigurationBuilder<K, V> configBuilder = CacheConfigurationBuilder
                .newCacheConfigurationBuilder(keyType, valueType, resourcePools);
        this.cache = persistentCacheManager.createCache("threeTieredCache", configBuilder);
    }

    @Override
    public int size() {
        return dataSetSize.get();
    }

    @Override
    public boolean isEmpty() {
        return dataSetSize.get() == 0;
    }

    @Override
    public boolean containsKey(@NonNull Object key) {
        return cache.containsKey((K) key);
    }

    @Override
    public boolean containsValue(@NonNull Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(@NonNull Object key) {
        return cache.get((K) key);
    }

    @Override
    public V put(@NonNull K key, @NonNull V value) {
        cache.put(key, value);
        dataSetSize.incrementAndGet();
        return value;
    }

    @Override
    public V remove(@NonNull Object key) {
        V value = cache.get((K) key);
        cache.remove((K) key);
        dataSetSize.decrementAndGet();
        return value;
    }

    @Override
    public void putAll(@NonNull Map<? extends K, ? extends V> map) {
        dataSetSize.addAndGet(map.size());
        cache.putAll(map);
    }

    @Override
    public void clear() {
        dataSetSize.set(0);
        cache.clear();
    }

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    public Stream<Entry<K, V>> entryStream() {
        return Streams.stream(cache.iterator())
                .map(entry -> new AbstractMap.SimpleEntry(entry.getKey(), entry.getValue()));
    }

}
