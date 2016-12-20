package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
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
    @Getter
    public String logDir;

    private LoadingCache<String, String> cache;

    private final long maxDataStoreSize = 1_000;


    public DataStore(Map<String, Object> opts) {
        this.logDir = (String) opts.get("--log-path");
        this.cache = Caffeine.newBuilder().writer(new CacheWriter<String, String>(
        ) {
            @Override
            public synchronized void write(@Nonnull String key, @Nonnull String value) {
                System.out.println("  *caffeine write " + key);
                if (logDir == null) {
                    return;
                }
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
                if (logDir == null) {
                    return;
                }
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
                    if (logDir != null) {
                        try {
                            Path path = Paths.get(logDir + File.separator + key);
                            if (Files.notExists(path)) {
                                return null;
                            }
                            return new String(Files.readAllBytes(path));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return null;
                })
               ;
    }

    public synchronized void handleEviction(String key, String  entry, RemovalCause cause) {
        System.out.println("  *caffeine eviction " + key + ", " + entry);
    }


        @Override
    public synchronized  <T> void put(Class<T> tClass, String prefix, String key, T value) {
        System.out.println("dataStore put " + getKey(prefix, key) + ", " + value);
        cache.put(getKey(prefix, key), JSONUtils.parser.toJson(value, tClass));
    }

    @Override
    public synchronized  <T> T get(Class<T> tClass, String prefix, String key) {
        System.out.println("dataStore get of " + getKey(prefix, key));
        String json = cache.get(getKey(prefix, key));
        System.out.println("dataStore get retrieves value " + getKey(prefix, key) + ", " + json);
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

    @Override
    public synchronized void deleteAll() {
        cache.invalidateAll();
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
