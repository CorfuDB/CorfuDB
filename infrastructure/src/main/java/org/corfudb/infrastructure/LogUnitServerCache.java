package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogUnitServer.LogUnitServerConfig;
import org.corfudb.infrastructure.log.InMemoryStreamLog;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;

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
public class LogUnitServerCache {

    private final LoadingCache<Long, ILogData> dataCache;
    private final StreamLog streamLog;

    public LogUnitServerCache(LogUnitServerConfig config, StreamLog streamLog) {
        this.streamLog = streamLog;
        this.dataCache = Caffeine.newBuilder()
                .<Long, ILogData>weigher((addr, logData) -> logData.getSizeEstimate())
                .maximumWeight(config.getMaxCacheSize())
                .removalListener(this::handleEviction)
                .build(this::handleRetrieval);
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
    private ILogData handleRetrieval(long address) {
        LogData entry = streamLog.read(address);
        log.debug("handleRetrieval: Retrieved[{} : {}]", address, entry);
        if (entry != null && !(streamLog instanceof InMemoryStreamLog)) {
            //System.out.println("Here2");
            entry.serializeMetadata();
        }
        return entry;
    }

    private void handleEviction(long address, ILogData entry, RemovalCause cause) {
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
    public ILogData get(long address, boolean cacheable) {
        if (!cacheable) {
            ILogData ld = dataCache.getIfPresent(address);
            return ld != null ? ld : handleRetrieval(address);
        }
        return dataCache.get(address);
    }

    /**
     * Returns the log entry from the cache or retrieves it from the underlying storage.
     *
     * @param address the address of the log entry to retrieve
     * @return the log entry read from cache or retrieved the underlying storage
     */
    public ILogData get(long address) {
        //System.out.println("Here1");
        return get(address, true);
    }

    /**
     * Puts the log entry into the cache.
     * {@link LoadingCache#put(Object, Object)}
     *
     * @param address the address to write entry to
     * @param entry   the log entry to write
     */
    public void put(long address, ILogData entry) {
        log.info("LogUnitServerCache.put: Cache write[{} : {}]", address, entry);
        if (entry instanceof LogData && !(streamLog instanceof InMemoryStreamLog)) {
            //System.out.println("In mem metadata deepSizeOf {}" + ((LogData) entry).getDeepSizeOfMetadata());
            ((LogData)entry).serializeMetadata();
            //System.out.println("Serialized Metadata numbytes {}" + ((LogData) entry).getSerializedMetadataBytes());
            //System.out.println("Total Size Estimate - " + entry.getSizeEstimate() + "Total Deep Size - " + sizeOf.deepSizeOf(entry));
        }
        dataCache.put(address, entry);
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
