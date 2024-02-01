package org.corfudb.runtime.object;

import lombok.Builder;
import lombok.Getter;
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
@Getter
@Builder
public class PersistenceOptions {
    public static final int DISABLE_BLOCK_CACHE = -1;
    private static final ConcurrentHashMap<Integer, Cache> blockCacheMap = new ConcurrentHashMap<>();

    synchronized public static Cache getBlockCAche(int blockCacheIndex) {
        if (!blockCacheMap.containsKey(blockCacheIndex)) {
            throw new NoSuchElementException("Specified block cache does not exist.");
        }

        return blockCacheMap.get(blockCacheIndex);
    }

    synchronized public static int newBlockCache(int size) {
        RocksDB.loadLibrary();

        int index = blockCacheMap.size();
        blockCacheMap.put(index, new LRUCache(size));
        return index;
    }

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
}
