package org.corfudb.runtime.object;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuOptions.ConsistencyModel;
import org.corfudb.runtime.CorfuOptions.SizeComputationModel;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;

import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reflects {@link CorfuOptions.PersistenceOptions}
 * since Protobuf does not allow for explicit default options
 */
@Slf4j
@Getter
@Builder
public class PersistenceOptions {
    public static final int DISABLE_BLOCK_CACHE = -1;
    private static final ConcurrentHashMap<Integer, Cache> blockCacheMap = new ConcurrentHashMap<>();

    Path dataPath;

    @Builder.Default
    ConsistencyModel consistencyModel = ConsistencyModel.READ_YOUR_WRITES;

    @Builder.Default
    SizeComputationModel sizeComputationModel = SizeComputationModel.EXACT_SIZE;

    @Builder.Default
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    Optional<Long> writeBufferSize = Optional.empty();

    @Builder.Default
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    Optional<Cache> blockCache = Optional.empty();

    @Builder.Default
    boolean disableBlockCache = false;

    public static synchronized Cache getBlockCache(int blockCacheIndex) {
        if (!blockCacheMap.containsKey(blockCacheIndex)) {
            throw new NoSuchElementException("Specified block cache does not exist.");
        }

        return blockCacheMap.get(blockCacheIndex);
    }

    /**
     * Create a new block cache and return its index. The client is
     * responsible for disposing of the object cache when no longer
     * needed. See {@link PersistenceOptions::disposeBlockCache}.
     * <p>
     * For usage patterns, please take a look at
     * CorfuStoreShimTest::testBlockCache.
     *
     * @param size The size of a block cache in bytes.
     * @return A handle to the new block cache.
     */
    synchronized public static int newBlockCache(int size) {
        synchronized (blockCacheMap) {
            RocksDB.loadLibrary();

            final int index = blockCacheMap.size();
            blockCacheMap.put(index, new LRUCache(size));
            log.info("Creating new block cache of size {} at index {}.", size, index);
            return index;
        }
    }

    /**
     * Dispose of the previously created block cache.
     *
     * @param index Previously return block cache index.
     */
    public static void disposeBlockCache(int index) {
        synchronized (blockCacheMap) {
            RocksDB.loadLibrary();

            if (index >= blockCacheMap.size() || index < 0) {
                log.info("Invalid block cache at index {}.", index);
                return;
            }

            if (blockCacheMap.get(index) == null) {
                log.info("Block cache at index {} already disposed.", index);
                return;
            }

            log.info("Disposing block cache at index {}.", index);
            blockCacheMap.remove(index).close();
        }
    }
}
