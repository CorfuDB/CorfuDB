package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.StatsGroup;
import org.corfudb.infrastructure.LogUnitServer.LogUnitServerConfig;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;

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

    //Size of key in the cache.  8 bytes as its a long
    private final int KEY_SIZE = 8;

    //Empirical threshold of number of streams in a logdata beyond which server performance may be slow
    private final int MAX_STREAM_THRESHOLD = 20;

    public LogUnitServerCache(LogUnitServerConfig config, StreamLog streamLog, StatsGroup stats) {
        this.streamLog = streamLog;
        this.dataCache = Caffeine.newBuilder()
                .<Long, ILogData>weigher((addr, logData) -> getLogDataTotalSize(logData))
                .maximumWeight(config.getMaxCacheSize())
                .removalListener(this::handleEviction)
                .build(this::handleRetrieval);

        StatsGroup cacheStats = stats.scope(getClass().getSimpleName());

        cacheStats.createGauge("hit_rate", this.dataCache.stats()::hitRate);
        cacheStats.createGauge("average_load_time", this.dataCache.stats()::averageLoadPenalty);
        cacheStats.createGauge("size_estimate", this.dataCache::estimatedSize);
        // TODO(Maithem) add total weight
    }

    private int getLogDataTotalSize(ILogData logData) {
        if (logData.getStreams().size() > MAX_STREAM_THRESHOLD) {
            log.warn("Number of streams in this data is higher that threshold {}." +
                    "This may impact the server performance", MAX_STREAM_THRESHOLD);
        }
        return logData.getSizeEstimate() + KEY_SIZE;
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
        log.trace("handleRetrieval: Retrieved[{} : {}]", address, entry);
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
     * Returns the log entry form the cache or retrieves it from the underlying storage.
     *
     * @param address the address of the log entry to retrieve
     * @return the log entry read from cache or retrieved the underlying storage
     */
    public ILogData get(long address) {
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
        log.trace("LogUnitServerCache.put: Cache write[{} : {}]", address, entry);
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
