package org.corfudb.infrastructure.datastore;

import static org.corfudb.infrastructure.utils.Persistence.syncDirectory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ClassUtils;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.paxos.PaxosDataStore;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.util.JsonUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Stores data as JSON.
 *
 * <p>Handle in-memory and persistent case differently:
 *
 * <p>In persistent mode, we use a {@link LoadingCache}, where an in-memory map is backed by disk.
 * In this scheme, the key for each value is also the name of the file where the value is stored.
 * The key is determined as (prefix + "_" + key).
 * The cache here serves mostly for easily managed synchronization of in-memory/file.
 * Some on-disk files will be deleted when their number exceed user specified limit.
 *
 * <p>Created by mdhawan on 7/27/16.
 */

@Slf4j
public class DataStore implements KvDataStore {

    public static final String EXTENSION = ".ds";

    @Getter
    private final Set<String> dsFilePrefixesForCleanup = Sets.newHashSet(
            PaxosDataStore.PREFIX_PHASE_1,
            PaxosDataStore.PREFIX_PHASE_2,
            ServerContext.PREFIX_LAYOUTS
    );

    @Getter
    private final Cache<String, Object> cache;

    @Getter
    private final DataStoreConfig config;

    /**
     * Lambda that cleans up DataStore files
     */
    private final Consumer<String> cleanupTask;

    /**
     * Returns a new DataStore object.
     *
     * @param config data store configuration parameters
     */
    public DataStore(DataStoreConfig config) {
        this.config = config;
        this.cleanupTask = this::dataStoreFileCleanup;
        this.cache = buildPersistentDs();
    }

    /**
     * Returns a new DataStore object.
     *
     * @param config      data store configuration parameters
     * @param cleanupTask method to cleanup DataStore files
     */
    public DataStore(DataStoreConfig config, Consumer<String> cleanupTask) {
        this.config = config;
        this.cleanupTask = cleanupTask;
        this.cache = buildPersistentDs();
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
                            String logDirPath = config.getLogDirPath();
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
                            Path path = Paths.get(config.getLogDirPath(), key);
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .maximumSize(config.getDsCacheSize())
                .build();
    }

    private <T> T load(Class<T> tClass, String key) {
        try {
            Path path = Paths.get(config.getLogDirPath(), key + EXTENSION);
            if (!path.toFile().exists()) {
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
            return JsonUtils.parser.fromJson(json, tClass);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Cleanup the DataStore files with names that are prefixes of the specified
     * fileName when so that the number of these files don't exceed the user-defined
     * retention limit. Cleanup is always done on files with lower epochs.
     */
    private void dataStoreFileCleanup(String fileName) {
        String logDirPath = config.getLogDirPath();

        if (logDirPath == null) {
            return;
        }

        File logDir = new File(logDirPath);
        Set<String> prefixesToClean = getDsFilePrefixesForCleanup();
        int numRetention = config.getMetadataRetention();

        prefixesToClean.stream()
                .filter(fileName::startsWith)
                .forEach(prefix -> {
                    File[] foundFiles = logDir.listFiles((dir, name) -> name.startsWith(prefix));
                    if (foundFiles == null || foundFiles.length <= numRetention) {
                        log.debug("DataStore cleanup not started for prefix: {}.", prefix);
                        return;
                    }
                    log.debug("Start cleaning up DataStore files with prefix: {}.", prefix);
                    Arrays.stream(foundFiles)
                            .sorted(Comparator.comparingInt(file -> {
                                // Extract epoch number from file name and cast to int for comparision
                                Matcher matcher = Pattern.compile("\\d+").matcher(file.getName());
                                return matcher.find(prefix.length()) ? Integer.parseInt(matcher.group()) : 0;
                            }))
                            .limit((long) foundFiles.length - (long) numRetention)
                            .forEach(file -> {
                                try {
                                    if (Files.deleteIfExists(file.toPath())) {
                                        log.info("Removed DataStore file: {}", file.getName());
                                    }
                                } catch (Exception e) {
                                    log.error("Error when cleaning up DataStore files", e);
                                }
                            });
                });
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
            T loadedVal = load(key.getDataType(), path);
            if (loadedVal != null) {
                return loadedVal;
            }

            // We need to maintain a path -> null mapping for keys that were loaded, but
            // were empty. This is required to prevent loading an empty key more than once, which is expensive.
            return NullValue.NULL_VALUE;
        });

        return val == NullValue.NULL_VALUE ? null : ClassUtils.cast(val);
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

    @Builder
    @Getter
    @ToString
    public static class DataStoreConfig {

        public static final String LOG_PATH_PARAM = "--log-path";
        public static final String MEMORY_PARAM = "--memory";
        public static final String METADATA_RETENTION_PARAM = "--metadata-retention";

        private static final int DEFAULT_METADATA_RETENTION = 1_000;

        @Default
        private final boolean inMemory = true;

        @NonNull
        @Default
        private final Optional<String> logDirPath = Optional.empty();

        @Default
        private final int metadataRetention = DEFAULT_METADATA_RETENTION;

        /**
         * Size bound for in-memory cache for dataStore
         */
        @Default
        private final long dsCacheSize = 1_000;

        /**
         * Parses raw config
         * @param opts parameters
         * @return data store type safe config
         */
        public static DataStoreConfig parse(Map<String, Object> opts) {
            boolean hasLogPath = opts.containsKey(LOG_PATH_PARAM);
            boolean isInMem = opts.containsKey(MEMORY_PARAM) && (Boolean) opts.get(MEMORY_PARAM);
            boolean inMem = isInMem || !hasLogPath;

            int numRetention = DEFAULT_METADATA_RETENTION;
            if (opts.containsKey(METADATA_RETENTION_PARAM)) {
                numRetention = Integer.parseInt((String) opts.get(METADATA_RETENTION_PARAM));
            }

            if (inMem) {
                return DataStoreConfig.builder()
                        .metadataRetention(numRetention)
                        .build();
            }

            return DataStoreConfig.builder()
                    .inMemory(false)
                    .logDirPath(Optional.of((String) opts.get(DataStoreConfig.LOG_PATH_PARAM)))
                    .metadataRetention(numRetention)
                    .build();
        }

        public String getLogDirPath() {
            if (!logDirPath.isPresent()) {
                throw new IllegalStateException("Log dir path is not presented. Config: " + this);
            }
            return logDirPath.get();
        }
    }
}
