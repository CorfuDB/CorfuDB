package org.corfudb.runtime.collections;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.StreamLog;

import static org.corfudb.util.MetricsUtils.sizeOf;

/**
 * LogUnit server cache.
 * <p>
 * All reads and writes go through this cache. But in some cases, messages can
 * specify non-cacheable read/write, then they will not go through this cache.
 * <p>
 * Created by WenbinZhu on 5/30/19.
 */
@Slf4j
public class TestCaffeinCache {

    private final LoadingCache<Long, String> dataCache;
    private final StreamLog streamLog;
    private long serializedSize = 0;

    public TestCaffeinCache(Long maxSize , StreamLog streamLog) {
        this.streamLog = streamLog;
        this.dataCache = Caffeine.newBuilder()
                .<Long, String>weigher((addr, logData) -> logData.length())
                .maximumWeight(maxSize)
                .removalListener(this::handleEviction)
                .build(this::handleRetrieval);
        System.out.print("\ninit Deepsize cache " +sizeOf.deepSizeOf(this.dataCache) +" maxWeight " + maxSize);
    }

    /**
     * Retrieves the LogUnitEntry from disk, given an address.
     *
     * @param address the address to retrieve the entry from
     * @return the log unit entry to retrieve into the cache
     * <p>
     * This function should not care about trimmed addresses, as that is handled
     * in the streamLog. Any address that cannot be retrieved should be returned
     * as un-written (null).
     */
    private String handleRetrieval(long address) {
        String entry = streamLog.read(address).toString();
        log.trace("handleRetrieval: Retrieved[{} : {}]", address, entry);
        return entry;
    }

    private void handleEviction(long address, String entry, RemovalCause cause) {
        log.trace("handleEviction: Eviction[{}]: {}", address, cause);
    }

    /**
     * Returns the log entry form the cache or retrieves it from the underlying storage.
     * <p>
     * If the log entry is not cacheable and it does not exist in cache, it will not be
     * cached when retrieved from the underlying storage.
     *
     * @param address   the address of the log entry to retrieve
     * @param cacheable if the log entry should be cached when retrieved from underlying storage
     * @return the log entry read from cache or retrieved the underlying storage
     */
    public String get(long address, boolean cacheable) {

            String ld = dataCache.getIfPresent(address);
            return ld;

    }

    /**
     * Returns the log entry form the cache or retrieves it from the underlying storage.
     *
     * @param address the address of the log entry to retrieve
     * @return the log entry read from cache or retrieved the underlying storage
     */
    public String get(long address) {
        return get(address, true);
    }

    /**
     * Puts the log entry into the cache.
     * {@link LoadingCache#put(Object, Object)}
     *
     * @param address the address to write entry to
     * @param entry   the log entry to write
     */
    public void put(long address, String entry) {
        log.trace("LogUnitServerCache.put: Cache write[{} : {}]", address, entry);
        dataCache.put(address, entry);
        serializedSize += entry.length();
            // System.out.print("\ncentry serialized size " + entry.length() + " deep size " + sizeOf.deepSizeOf(entry));
            //System.out.print("\ncache num_element " + dataCache.estimatedSize() + " deepSize " + sizeOf.deepSizeOf(dataCache));
    }

    /**
     * Discards all the entries in the cache.
     * {@link LoadingCache#invalidateAll()}
     */
    public void invalidateAll() {
        dataCache.invalidateAll();
    }

    @VisibleForTesting
    public int getSize() {
        return dataCache.asMap().size();
    }
}
