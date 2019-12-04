package org.corfudb.infrastructure.datastore;

import static org.corfudb.infrastructure.utils.Persistence.syncDirectory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.configuration.ServerConfiguration;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.util.JsonUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.function.Consumer;

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
 * Some on-disk files will be deleted when their number exceed user specified limit.
 *
 * <p>If 'opts' either has '--memory=true' or a log-path for storing files is not provided,
 * the store is just an in memory cache.
 *
 * <p>Created by mdhawan on 7/27/16.
 */

@Slf4j
public class DataStore implements KvDataStore {

    public static final String EXTENSION = ".ds";

    @Getter
    private final Cache<String, Object> cache;

    @Getter
    private final String logDirPath;

    @Getter
    private final long dsCacheSize = 1_000; // size bound for in-memory cache for dataStore

    private final boolean inMem;

    private final Consumer<String> cleanupTask;

    /**
     * Return a new DataStore object.
     *
     * @param conf        server configuration
     * @param cleanupTask method to cleanup DataStore files
     */
    public DataStore(@Nonnull ServerConfiguration conf,
                     @Nonnull Consumer<String> cleanupTask) {

        if (conf.isInMemoryMode()) {
            this.logDirPath = null;
            this.cleanupTask = fileName -> { };
            cache = buildMemoryDs();
            inMem = true;
        } else {
            this.logDirPath = conf.getServerDir();
            this.cleanupTask = cleanupTask;
            cache = buildPersistentDs();
            inMem = false;
        }
    }

    /**
     * obtain an in-memory cache, no content loader, no writer, no size limit.
     *
     * @return new LoadingCache for the DataStore
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
     * The cache is backed up by file-per-key under {@link DataStore::logDirPath}.
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
                            String dsFileName = key + EXTENSION;
                            Path path = Paths.get(logDirPath, dsFileName);
                            Path tmpPath = Paths.get(logDirPath, dsFileName + ".tmp");

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
                            syncDirectory(logDirPath);
                            // Invoking the cleanup on each disk file write is fine for performance
                            // since DataStore files are not supposed to change too frequently
                            cleanupTask.accept(dsFileName);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public synchronized void delete(@Nonnull String key,
                                                    @Nullable Object value,
                                                    @Nonnull RemovalCause cause) {
                        try {
                            Path path = Paths.get(logDirPath, key);
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .maximumSize(dsCacheSize)
                .build();
    }

    private <T> T load(Class<T> tClass, String key) {
        try {
            Path path = Paths.get(logDirPath, key + EXTENSION);
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
    public synchronized <T> void put(KvRecord<T> key, T value) {
        cache.put(key.getFullKeyName(), value);
    }

    @Override
    public synchronized <T> T get(KvRecord<T> key) {
        String path = key.getFullKeyName();
        Object val = cache.get(path, k -> {
            if (!inMem) {
                T loadedVal = load(key.getDataType(), path);
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
    public <T> T get(KvRecord<T> key, T defaultValue) {
        T value = get(key);
        return value == null ? defaultValue : value;
    }

    @Override
    public synchronized <T> void delete(KvRecord<T> key) {
        cache.invalidate(key.getFullKeyName());
    }
}
