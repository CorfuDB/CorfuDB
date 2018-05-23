package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import lombok.Getter;

import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.util.JsonUtils;

import static org.corfudb.infrastructure.utils.Persistence.syncDirectory;

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

    static String EXTENSION = ".ds";

    @Getter
    private final Cache<String, Object> cache;
    private final String logDir;

    @Getter
    private final long dsCacheSize = 1_000; // size bound for in-memory cache for dataStore

    private final boolean inMem;

    /**
     * Return a new DataStore object.
     * @param opts  map of option strings
     */
    public DataStore(Map<String, Object> opts) {

        if ((opts.get("--memory") != null && (Boolean) opts.get("--memory"))
                || opts.get("--log-path") == null) {
            this.logDir = null;
            cache = buildMemoryDs();
            inMem = true;
        } else {
            this.logDir = (String) opts.get("--log-path");
            cache = buildPersistentDs();
            inMem = false;
        }
    }

    /**
     * obtain an in-memory cache, no content loader, no writer, no size limit.
     * @return  new LoadingCache for the DataStore
     */
    private Cache<String, Object> buildMemoryDs() {
        return Caffeine.newBuilder().build(k -> null);
    }


    public static int getChecksum(byte[] bytes) {
        Hasher hasher = Hashing.crc32c().newHasher();
        for (byte a : bytes) {
            hasher.putByte(a);
        }

        return hasher.hash().asInt();
    }

    /**
     * obtain a {@link LoadingCache}.
     * The cache is backed up by file-per-key uner {@link DataStore::logDir}.
     * The cache size is bounded by {@link DataStore::dsCacheSize}.
     *
     * @return the cache object
     */
    private Cache<String, Object> buildPersistentDs() {
        return Caffeine.newBuilder()
                .recordStats()
                .writer(new CacheWriter<String, Object>() {
                    @Override
                    public synchronized void write(@Nonnull String key, @Nonnull Object value) {

                        if (value == NullValue.NULL_VALUE) {
                            return;
                        }

                        try {
                            Path path = Paths.get(logDir + File.separator + key + EXTENSION);
                            Path tmpPath = Paths.get(logDir + File.separator + key + EXTENSION + ".tmp");

                            String jsonPayload = JsonUtils.parser.toJson(value, value.getClass());
                            byte[] bytes = jsonPayload.getBytes();

                            ByteBuffer buffer = ByteBuffer.allocate(bytes.length
                                    + Integer.BYTES);
                            buffer.putInt(getChecksum(bytes));
                            buffer.put(bytes);
                            Files.write(tmpPath, buffer.array(), StandardOpenOption.CREATE,
                                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);
                            Files.move(tmpPath, path, StandardCopyOption.REPLACE_EXISTING,
                                    StandardCopyOption.ATOMIC_MOVE);
                            syncDirectory(logDir);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public synchronized void delete(@Nonnull String key,
                                                    @Nullable Object value,
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
                .build();
    }

    @Override
    public synchronized  <T> void put(Class<T> tclass, String prefix, String key, T value) {
        cache.put(getKey(prefix, key), value);
    }

    private <T> T load(Class<T> tClass, String key) {
        try {
            Path path = Paths.get(logDir + File.separator + key + EXTENSION);
            if (Files.notExists(path)) {
                return null;
            }
            byte[] bytes = Files.readAllBytes(path);
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            int checksum = buf.getInt();
            byte[] strBytes = Arrays.copyOfRange(bytes, 4, bytes.length);
            if (checksum != getChecksum(strBytes)) {
                throw new DataCorruptionException();
            }

            String json = new String(strBytes);
            T val = JsonUtils.parser.fromJson(json, tClass);
            return val;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Since the cache can't maintain key->null mappings, this enum
     * is a place holder for null to allow keys to map to null.
     */
    private enum NullValue {
        NULL_VALUE
    }

    @Override
    public synchronized <T> T get(Class<T> tclass, String prefix, String key) {
        String path = getKey(prefix, key);
        Object val = cache.get(path, k -> {
            if (!inMem) {
                T loadedVal = load(tclass, path);
                if (loadedVal != null) {
                    return loadedVal;
                }
            }

            // We need to maintain a path -> null mapping for keys that were loaded, but
            // were empty. This is required to prevent loading an empty key more than once, which is expensive.
            return NullValue.NULL_VALUE;
        });
        
        return val == NullValue.NULL_VALUE ? null : (T) val;
    }

    @Override
    public synchronized <T> void delete(Class<T> tclass, String prefix, String key) {
        cache.invalidate(getKey(prefix, key));
    }

    private String getKey(String prefix, String key) {
        return prefix + "_" + key;
    }
}
