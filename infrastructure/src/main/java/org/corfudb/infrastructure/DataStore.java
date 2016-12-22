package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.*;
import lombok.Getter;
import org.corfudb.util.JSONUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stores data as JSON.
 *
 * Handle in-memory and persistent case differently:
 *
 * In in-memory mode, the "cache" is actually the store, so we never evict anything from it.
 *
 * In persistent mode, we use a {@link LoadingCache}, where an in-memory map is backed by disk.
 * In this scheme, the key for each value is also the name of the file where the value is stored.
 * The key is determined as (prefix + "_" + key).
 * The cache here serves mostly for easily managed synchronization of in-memory/file.
 * <p>
 * If 'opts' either has '--memory=true' or a log-path for storing files is not provided,
 * the store is just an in memory cache.
 * <p>
 * Created by mdhawan on 7/27/16.
 */
public class DataStore implements IDataStore {

    private final Map<String, Object> opts;
    private final boolean isPersistent;
    private final LoadingCache<String, String> cache;
    private final String logDir;

    @Getter
    private final long DS_CACHE_SZ = 1_000; // size bound for in-memory cache for dataStore


    public DataStore(Map<String, Object> opts) {
        this.opts = opts;

        if ((opts.get("--memory") != null && (Boolean) opts.get("--memory"))
                || opts.get("--log-path") == null) {
            // in-memory dataSture case
            isPersistent = false;
            this.logDir = null;
            cache = buildMemoryDS();
        } else {
            // persistent dataSture case
            isPersistent = true;
            this.logDir = (String) opts.get("--log-path");
            cache = buildPersistentDS();
        }
    }

    /**
     * obtain an in-memory cache, no content loader, no writer, no size limit.
     * @return
     */
    private LoadingCache<String, String> buildMemoryDS() {
        LoadingCache<String, String> cache = Caffeine
                .newBuilder()
                .build(k -> null);
        return cache;
    }

    /**
     * obtain a {@link LoadingCache}.
     * The cache is backed up by file-per-key uner {@link DataStore::logDir}.
     * The cache size is bounded by {@link DataStore::DS_CACHE_SZ}.
     *
     * @return the cache object
     */
    private LoadingCache<String, String> buildPersistentDS() {
        LoadingCache<String, String> cache = Caffeine.newBuilder()
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
                    public synchronized void delete(@Nonnull String key, @Nullable String value, @Nonnull RemovalCause cause) {
                        try {
                            Path path = Paths.get(logDir + File.separator + key);
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
            .maximumSize(DS_CACHE_SZ)
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
    public synchronized  <T> void put(Class<T> tClass, String prefix, String key, T value) {
        cache.put(getKey(prefix, key), JSONUtils.parser.toJson(value, tClass));
    }

    @Override
    public synchronized  <T> T get(Class<T> tClass, String prefix, String key) {
        String json = cache.get(getKey(prefix, key));
        return getObject(json, tClass);
    }

    /**
     * This is an atomic conditional get/put: If the key is not found, then the specified value is inserted.
     * It returns the latest value, either the original one found, or the newly inserted value
     * @param tClass type of value
     * @param prefix prefix part of key
     * @param key suffice part of key
     * @param value value to be conditionally accepted
     * @param <T> value class
     * @return the latest value in the cache
     */
    public <T> T get(Class<T> tClass, String prefix, String key, T value) {
        String keyString = getKey(prefix, key);
        String json = cache.get(keyString, k -> JSONUtils.parser.toJson(value, tClass));
        return getObject(json, tClass);
    }

    @Override
    public synchronized  <T> List<T> getAll(Class<T> tClass, String prefix) {
        List<T> list = new ArrayList<T>();
        for (Map.Entry<String, String> entry : cache.asMap().entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                list.add(getObject(entry.getValue(), tClass));
            }
        }
        return list;
    }

    @Override
    public synchronized <T> void delete(Class<T> tClass, String prefix, String key) {
        cache.invalidate(getKey(prefix, key));
    }

    // Helper methods

    private <T> T getObject(String json, Class<T> tClass) {
        return isNotNull(json) ? JSONUtils.parser.fromJson(json, tClass) : null;
    }

    private String getKey(String prefix, String key) {
        return prefix + "_" + key;
    }

    private boolean isNotNull(String value) {
        return value != null && !value.trim().isEmpty();
    }

}
