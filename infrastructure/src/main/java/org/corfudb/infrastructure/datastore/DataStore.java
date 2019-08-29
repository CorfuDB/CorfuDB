package org.corfudb.infrastructure.datastore;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.util.JsonUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.function.Consumer;

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
    private final String logDirPath;

    @Getter
    private final long dsCacheSize = 1_000; // size bound for in-memory cache for dataStore

    private final boolean inMem;

    private final Consumer<String> cleanupTask;

    /**
     * Return a new DataStore object.
     *
     * @param opts        map of option strings
     * @param cleanupTask method to cleanup DataStore files
     */
    public DataStore(@Nonnull Map<String, Object> opts,
                     @Nonnull Consumer<String> cleanupTask) {

        if ((opts.containsKey("--memory") && (Boolean) opts.get("--memory")) || !opts.containsKey("--log-path")) {
            this.logDirPath = null;
            this.cleanupTask = fileName -> { };
            cache = buildMemoryDs();
            inMem = true;
        } else {
            this.logDirPath = (String) opts.get("--log-path");
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


    private static int getChecksum(ByteBuffer buf) {
        Hasher hasher = Hashing.crc32c().newHasher();
        buf.mark();
        hasher.putBytes(buf);
        buf.reset();

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

                            ByteBuffer dataBuf;
                            if (value instanceof ByteBuffer) {
                                dataBuf = ((ByteBuffer) value);
                            } else {
                                String jsonPayload = JsonUtils.parser.toJson(value, value.getClass());
                                dataBuf = ByteBuffer.wrap(jsonPayload.getBytes());
                            }

                            ByteBuffer checksumBuf = ByteBuffer.allocate(Integer.BYTES);
                            checksumBuf.putInt(getChecksum(dataBuf));
                            checksumBuf.flip();
                            ByteBuffer[] buffers = {checksumBuf, dataBuf};

                            FileChannel writeChannel = FileChannel.open(tmpPath,
                                    StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

                            while (checksumBuf.hasRemaining() || dataBuf.hasRemaining()) {
                                writeChannel.write(buffers);
                            }

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

            FileChannel readChannel = FileChannel.open(path, StandardOpenOption.READ);
            ByteBuffer dataBuf = ByteBuffer.allocate((int) readChannel.size());
            readChannel.read(dataBuf);
            dataBuf.flip();

            int checksum = dataBuf.getInt();
            if (checksum != getChecksum(dataBuf)) {
                throw new DataCorruptionException();
            }

            if (tClass.equals(ByteBuffer.class)) {
                return (T) dataBuf;
            }

            String json = new String(dataBuf.array(), Integer.BYTES, dataBuf.remaining());
            return JsonUtils.parser.fromJson(json, tClass);

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

    @Override
    public void deleteFiles(FileFilter fileFilter) {
        File[] files = new File(logDirPath).listFiles(fileFilter);

        if (files == null) {
            throw new RuntimeException("DataStore: Failed to list files to delete.");
        }

        for (File file : files) {
            if (file.delete()) {
                log.debug("DataStore: Deleted existing DataStore file: {}", file);
            }
        }
    }
}
