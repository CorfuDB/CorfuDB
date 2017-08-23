package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.Getter;

import org.corfudb.util.JsonUtils;

/**
 * Stores data as JSON.
 *
 * <p>Handle in-memory and persistent case differently:
 *
 * <p>In in-memory mode, the "cache" is actually the store, so we never evict anything from it.
 *
 * <p>In persistent mode, we use a {@link LoadingCache}, where an in-memory map is backed by disk.
 * In this scheme, the key for each value is also the name of the file where the value is stored.
 * The key is determined as (prefix + "_" + key).
 * The cache here serves mostly for easily managed synchronization of in-memory/file.
 *
 * <p>If 'opts' either has '--memory=true' or a log-path for storing files is not provided,
 * the store is just an in memory cache.
 *
 * <p>Created by mdhawan on 7/27/16.
 */
public class DataStore implements IDataStore {

    private final Map<String, Object> opts;
    private final boolean isPersistent;
    @Getter
    private final LoadingCache<String, String> cache;
    private final String logDir;

    @Getter
    private final long dsCacheSize = 1_000; // size bound for in-memory cache for dataStore

    /**
     * Return a new DataStore object.
     * @param opts  map of option strings
     */
    public DataStore(Map<String, Object> opts) {
        this.opts = opts;

        if ((opts.get("--memory") != null && (Boolean) opts.get("--memory"))
                || opts.get("--log-path") == null) {
            // in-memory dataSture case
            isPersistent = false;
            this.logDir = null;
            cache = buildMemoryDs();
        } else {
            // persistent dataSture case
            isPersistent = true;
            this.logDir = (String) opts.get("--log-path");
            cache = buildPersistentDs();
        }
    }

    /**
     * obtain an in-memory cache, no content loader, no writer, no size limit.
     * @return  new LoadingCache for the DataStore
     */
    private LoadingCache<String, String> buildMemoryDs() {
        LoadingCache<String, String> cache = Caffeine
                .newBuilder()
                .build(k -> null);
        return cache;
    }

    /**
     * obtain a {@link LoadingCache}.
     * The cache is backed up by file-per-key uner {@link DataStore::logDir}.
     * The cache size is bounded by {@link DataStore::dsCacheSize}.
     *
     * @return the cache object
     */
    private LoadingCache<String, String> buildPersistentDs() {
        LoadingCache<String, String> cache = Caffeine.newBuilder()
                .recordStats()
                .writer(new CacheWriter<String, String>() {
                    @Override
                    public synchronized void write(@Nonnull String key, @Nonnull String value) {
                        try {
                            Path path = Paths.get(logDir + File.separator + key);
                            Files.write(path, value.getBytes());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public synchronized void delete(@Nonnull String key,
                                                    @Nullable String value,
                                                    @Nonnull RemovalCause cause) {
                        try {
                            Path path = Paths.get(logDir + File.separator + key);
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .maximumSize(dsCacheSize)
                .build(key -> {
                    try {
                        Path path = Paths.get(logDir + File.separator + key);
                        if (Files.notExists(path)) {
                            return null;
                        }
                        return new String(Files.readAllBytes(path));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        return cache;
    }

    @Override
    public synchronized  <T> void put(Class<T> tclass, String prefix, String key, T value) {
        cache.put(getKey(prefix, key), JsonUtils.parser.toJson(value, tclass));
    }

    @Override
    public synchronized  <T> T get(Class<T> tclass, String prefix, String key) {
        String json = cache.get(getKey(prefix, key));
        return getObject(json, tclass);
    }

    /**
     * This is an atomic conditional get/put: If the key is not found,
     * then the specified value is inserted.
     * It returns the latest value, either the original one found,
     * or the newly inserted value
     * @param tclass type of value
     * @param prefix prefix part of key
     * @param key suffice part of key
     * @param value value to be conditionally accepted
     * @param <T> value class
     * @return the latest value in the cache
     */
    public <T> T get(Class<T> tclass, String prefix, String key, T value) {
        String keyString = getKey(prefix, key);
        String json = cache.get(keyString, k -> JsonUtils.parser.toJson(value, tclass));
        return getObject(json, tclass);
    }

    @Override
    public synchronized  <T> List<T> getAll(Class<T> tclass, String prefix) {
        List<T> list = new ArrayList<T>();
        for (Map.Entry<String, String> entry : cache.asMap().entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                list.add(getObject(entry.getValue(), tclass));
            }
        }
        return list;
    }

    @Override
    public synchronized <T> void delete(Class<T> tclass, String prefix, String key) {
        cache.invalidate(getKey(prefix, key));
    }

    // Helper methods

    private <T> T getObject(String json, Class<T> tclass) {
        return isNotNull(json) ? JsonUtils.parser.fromJson(json, tclass) : null;
    }

    private String getKey(String prefix, String key) {
        return prefix + "_" + key;
    }

    private boolean isNotNull(String value) {
        return value != null && !value.trim().isEmpty();
    }

}
