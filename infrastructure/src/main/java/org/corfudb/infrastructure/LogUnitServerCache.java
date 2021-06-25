package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.common.util.Memory;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.Optional;

import java.util.function.Supplier;

import static java.lang.Math.toIntExact;

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


    private final String loadTimeName = "logunit.cache.load_time";
    private final String hitRatioName = "logunit.cache.hit_ratio";
    private final String weightName = "logunit.cache.weight";

    public LogUnitServerCache(StreamLog streamLog, long maxSize) {
        this.streamLog = streamLog;
        this.dataCache = Caffeine.newBuilder()
                .<Long, ILogData>weigher((addr, logData) -> getLogDataTotalSize(logData))
                .maximumWeight(maxSize)
                .recordStats()
                .executor(Runnable::run)
                .removalListener(this::handleEviction)
                .build(this::handleRetrieval);

        MeterRegistryProvider.getInstance().ifPresent(registry ->
                CaffeineCacheMetrics.monitor(registry, dataCache, "logunit.read_cache"));
        MeterRegistryProvider.getInstance().map(registry ->
                Gauge.builder(hitRatioName,
                dataCache, cache -> cache.stats().hitRate()).register(registry));
        MeterRegistryProvider.getInstance().map(registry ->
                Gauge.builder(loadTimeName,
                        dataCache, cache -> cache.stats().totalLoadTime())
                        .register(registry));
        MeterRegistryProvider.getInstance().map(registry ->
                Gauge.builder(weightName,
                        dataCache, cache -> cache.stats().evictionWeight())
                        .register(registry));
    }

    private int getLogDataTotalSize(ILogData logData) {
        if (logData.getStreams().size() > MAX_STREAM_THRESHOLD) {
            log.warn("Number of streams in this data is higher that threshold {}." +
                "This may impact the server performance", MAX_STREAM_THRESHOLD);
        }

        long result = Math.addExact(logData.getSizeEstimate(), (Memory.sizeOf.deepSizeOf(logData.getMetadataMap())));
        return toIntExact(Math.addExact(result, KEY_SIZE));
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
        LogData entry = MicroMeterUtils.time(() -> streamLog.read(address), "logunit.read.timer");
        log.trace("handleRetrieval: Retrieved[{} : {}]", address, entry);
        return entry;
    }

    private void handleEviction(long address, ILogData entry, RemovalCause cause) {
        if (log.isTraceEnabled()) {
            log.trace("handleEviction: Eviction[{}]: {}", address, cause);
        }
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
        cleanUpGauges();
    }

    @VisibleForTesting
    public int getSize() {
        return dataCache.asMap().size();
    }

    private void cleanUpGauges() {
        String  [] names = new String[] {loadTimeName, hitRatioName, weightName};
        for (String gaugeName: names) {
            MeterRegistryProvider.deregisterServerMeter(gaugeName, Tags.empty(), Meter.Type.GAUGE);
        }
    }
}
