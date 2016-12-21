package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.*;
import com.google.common.cache.CacheBuilder;
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
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores data as JSON.
 * Employs a {@link LoadingCache} to front a file per value storage scheme.
 * In this scheme, the key for each value is also the name of the file where the value is stored.
 * The key is determined as (prefix + "_" + key).
 * <p>
 * If a log-path for storing files is not provided, the store is just an in memory cache.
 * <p>
 * Created by mdhawan on 7/27/16.
 */

public class DataStore implements IDataStore {

    private final Map<String, Object> opts;
    private final boolean isPersistent;
    private final Cache<String, String> cache;
    private final String logDir;

    @Getter
    private final long maxDataStoreSize = 1_000;


    public DataStore(Map<String, Object> opts) {
        this.opts = opts;

        if ((opts.get("--memory") != null && (Boolean) opts.get("--memory"))
                || opts.get("--log-path") == null) {
            isPersistent = false;
            this.logDir = null;
            cache = buildMemoryDS();
        } else {
            isPersistent = true;
            this.logDir = (String) opts.get("--log-path");
            cache = buildPersistentDS();
        }
    }

    private Cache<String, String> buildMemoryDS() {
        Cache<String, String> cache = Caffeine
                .newBuilder()
                .build();
        return cache;
    }

    private Cache<String, String> buildPersistentDS() {
        LoadingCache<String, String> cache = Caffeine.newBuilder()
                .writer(new CacheWriter<String, String>() {
                    @Override
                    public synchronized void write(@Nonnull String key, @Nonnull String value) {
                        System.out.println("  *caffeine write " + key);
                        try {
                            Path path = Paths.get(logDir + File.separator + key);
                            Files.write(path, value.getBytes());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public synchronized void delete(@Nonnull String key, @Nullable String value, @Nonnull RemovalCause cause) {
                        System.out.println("  *caffeine evict " + key);
                        try {
                            Path path = Paths.get(logDir + File.separator + key);
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
            .maximumSize(maxDataStoreSize)
            .removalListener(this::handleEviction)
            .build(key -> {
                    System.out.println("  *caffeine build " + key);
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

    public synchronized void handleEviction(String key, String  entry, RemovalCause cause) {
        System.out.println("  *caffeine eviction " + key + ", " + entry);
        if (! isPersistent)
            throw new RuntimeException(); // panic, we'll lose state!
    }


    @Override
    public synchronized  <T> void put(Class<T> tClass, String prefix, String key, T value) {
        System.out.println("dataStore put " + getKey(prefix, key) + ", " + value);
        cache.put(getKey(prefix, key), JSONUtils.parser.toJson(value, tClass));
    }

    @Override
    public synchronized  <T> T get(Class<T> tClass, String prefix, String key) {
        System.out.println("dataStore get of " + getKey(prefix, key));
        String json = cache.get(getKey(prefix, key), k -> null);
        if (json == null)
            System.out.println("dataStore get retrieves null value " + getKey(prefix, key));
        return getObject(json, tClass);
    }

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
