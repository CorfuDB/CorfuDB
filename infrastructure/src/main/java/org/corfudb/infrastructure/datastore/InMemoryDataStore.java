package org.corfudb.infrastructure.datastore;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Builder;
import org.corfudb.common.util.ClassUtils;

import java.util.Optional;

/**
 * In memory data store.
 * In in-memory mode, the "cache" is actually the store, so we never evict anything from it.
 * <p>
 * If 'opts' either has '--memory=true' or a log-path for storing files is not provided,
 * * the store is just an in memory cache.
 */
@Builder
public class InMemoryDataStore implements KvDataStore {

    /**
     * In-memory cache, no content loader, no writer, no size limit
     */
    private final Cache<String, Object> cache = Caffeine.newBuilder().build();

    /**
     * {@inheritDoc} Persists a value
     *
     * @param key   record meta information
     * @param value Immutable value (or a value that won't be changed)
     * @param <T>   value type
     */
    @Override
    public synchronized <T> void put(KvRecord<T> key, T value) {
        cache.put(key.getFullKeyName(), value);
    }

    /**
     * {@inheritDoc} Returns a value by the key
     *
     * @param key record meta information
     * @param <T> value type
     * @return value
     */
    @Override
    public <T> T get(KvRecord<T> key) {
        return get(key, null);
    }

    /**
     * {@inheritDoc} Returns a value by the key
     *
     * @param key          key meta info
     * @param defaultValue a default value
     * @param <T>          value type
     * @return value
     */
    @Override
    public <T> T get(KvRecord<T> key, T defaultValue) {
        String path = key.getFullKeyName();
        return Optional
                .ofNullable(cache.getIfPresent(path))
                .map(ClassUtils::<T>cast)
                .orElse(defaultValue);
    }

    /**
     * {@inheritDoc} deletes a value from the storage
     *
     * @param key record meta information
     * @param <T> value type
     */
    @Override
    public synchronized <T> void delete(KvRecord<T> key) {
        cache.invalidate(key.getFullKeyName());
    }
}
