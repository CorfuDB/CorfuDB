package org.corfudb.runtime.view;

import com.codahale.metrics.Gauge;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;


/**
 * A view of the address space implemented by Corfu.
 * <p>
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class AddressSpaceView extends AbstractView {

    /**
     * A cache for read results.
     */
    static LoadingCache<Long, ILogData> readCache;

    /**
     * Duration before retrying an empty read.
     */
    @Getter
    @Setter
    Duration emptyDuration = Duration.ofMillis(100L);

    public AddressSpaceView(CorfuRuntime runtime) {
        super(runtime);
        // We don't lock readCache, this should be ok in the rare
        // case we generate a second readCache as it won't be pointed to.
        if (readCache == null) {
            resetCaches();
        } else {
            log.debug("Read cache already built, re-using existing read cache.");
        }

        final String pfx = String.format("%s0x%x.cache.", runtime.getMpASV(), this.hashCode());
        runtime.getMetrics().register(pfx + "cache-size", (Gauge<Long>) () -> readCache.estimatedSize());
        runtime.getMetrics().register(pfx + "evictions", (Gauge<Long>) () -> readCache.stats().evictionCount());
        runtime.getMetrics().register(pfx + "hit-rate", (Gauge<Double>) () -> readCache.stats().hitRate());
        runtime.getMetrics().register(pfx + "hits", (Gauge<Long>) () -> readCache.stats().hitCount());
        runtime.getMetrics().register(pfx + "misses", (Gauge<Long>) () -> readCache.stats().missCount());
    }

    /**
     * Reset all in-memory caches.
     */
    public void resetCaches() {
        readCache = Caffeine.<Long, ILogData>newBuilder()
                .<Long, ILogData>weigher((k, v) -> v.getSizeEstimate())
                .maximumWeight(runtime.getMaxCacheSize())
                .recordStats()
                .build(new CacheLoader<Long, ILogData>() {
                    @Override
                    public ILogData load(Long aLong) throws Exception {
                        return cacheFetch(aLong);
                    }

                    @Override
                    public Map<Long, ILogData>
                    loadAll(Iterable<? extends Long> keys) throws Exception {
                        return cacheFetch((Iterable<Long>) keys);
                    }
                });
    }

    /** Write the given log data using a token, returning
     * either when the write has been completed successfully,
     * or throwing an OverwriteException if another value
     * has been adopted, or a WrongEpochException if the
     * token epoch is invalid.
     *
     * @param token     The token to use for the write.
     * @param data      The data to write.
     * @throws OverwriteException   If the globalAddress given
     *                              by the token has adopted
     *                              another value.
     * @throws WrongEpochException  If the token epoch is invalid.
     */
    public void write(IToken token, Object data)
        throws OverwriteException {
        final LogData ld = new LogData(data);
        layoutHelper(l -> {
            // Check if the token issued is in the same
            // epoch as the layout we are about to write
            // to.
            if (token.getEpoch() != l.getEpoch()) {
                throw new WrongEpochException(l.getEpoch());
            }

            // Set the data to use the token
            ld.useToken(token);

            // Do the write
            l.getReplicationMode(token.getTokenValue())
                        .getReplicationProtocol(runtime)
                        .write(l, ld);

            return null;
         });


        // Cache the successful write
        if (!runtime.isCacheDisabled()) {
            readCache.put(token.getTokenValue(), ld);
        }
    }

    /** Directly read from the log, returning any
     * committed value, or NULL, if no value has
     * been committed.
     *
     * @param address   The address to read from.
     * @return          Committed data stored in the
     *                  log, or NULL, if no value
     *                  has been committed.
     */
    public @Nullable ILogData peek(final long address) {
        return layoutHelper(l -> l.getReplicationMode(address)
                    .getReplicationProtocol(runtime)
                    .peek(l, address));
    }

    /**
     * Read the given object from an address and streams.
     *
     * @param address An address to read from.
     * @return A result, which be cached.
     */
    public @Nonnull ILogData read(long address) {
        if (!runtime.isCacheDisabled()) {
            ILogData data = readCache.get(address);
            if (data == null || data.getType() == DataType.EMPTY) {
                throw new RuntimeException("Unexpected return of empty data at address " + address + " on read");
            }
            return data;
        }
        return fetch(address);
    }

    @Deprecated
    public Map<Long, LogData> read(UUID stream, long offset, long size) {
        // TODO: We are assuming that we are reading from the most recent segment....
        return layoutHelper(l -> AbstractReplicationView
                        .getReplicationView(l, l.getSegments().get(l.getSegments().size() - 1).getReplicationMode(),
                                l.getSegments().get(l.getSegments().size() - 1))
                        .read(stream, offset, size)
        );
    }

    /**
     * Read the given object from a range of addresses.
     *
     * @param addresses An address range to read from.
     * @return A result, which be cached.
     */
    public Map<Long, ILogData> read(RangeSet<Long> addresses) {

        if (!runtime.isCacheDisabled()) {
            return readCache.getAll(Utils.discretizeRangeSet(addresses));
        }
        return this.cacheFetch(Utils.discretizeRangeSet(addresses));
    }

    public Map<Long, ILogData> read(List<Long> addresses) {

        if (!runtime.isCacheDisabled()) {
            return readCache.getAll(addresses);
        }
        return this.cacheFetch(addresses);
    }

    /**
     * Fetch an address for insertion into the cache.
     *
     * @param address An address to read from.
     * @return A result to be cached. If the readresult is empty,
     * This entry will be scheduled to self invalidate.
     */
    private @Nonnull ILogData cacheFetch(long address) {
        log.trace("Cache miss @ {}, fetching.", address);
        ILogData result = fetch(address);
        if (result.getType() == DataType.EMPTY) {
            throw new RuntimeException("Unexpected empty return at " +  address + " from fetch");
        }
        return result;
    }

    /**
     * Fetch an address for insertion into the cache.
     *
     * @param addresses An address to read from.
     * @return A result to be cached. If the readresult is empty,
     * This entry will be scheduled to self invalidate.
     */
    private Map<Long, ILogData> cacheFetch(Iterable<Long> addresses) {
        final ImmutableMap.Builder<Long, ILogData> dataBuilder = ImmutableMap.builder();

        addresses.forEach(a ->
            layoutHelper(l ->
                    dataBuilder.put(a, l.getReplicationMode(a).getReplicationProtocol(runtime)
            .read(l, a))));

        return dataBuilder.build();
    }


    /**
     * Explicitly fetch a given address, bypassing the cache.
     *
     * @param address An address to read from.
     * @return A result, which will be uncached.
     */
    public ILogData fetch(final long address) {
        return layoutHelper(l -> l.getReplicationMode(address)
                .getReplicationProtocol(runtime)
                .read(l, address)
        );
    }

    /**
     * Fill a hole at the given address.
     *
     * @param address An address to hole fill at.
     */
    @Deprecated
    public void fillHole(long address)
            throws OverwriteException {
        layoutHelper(
                l -> {
                    AbstractReplicationView
                            .getReplicationView(l, l.getReplicationMode(address), l.getSegment(address))
                            .fillHole(address);
                    return null;
                }
        );
    }

    @Deprecated
    public void fillStreamHole(UUID streamID, long address)
            throws OverwriteException {
        layoutHelper(
                l -> {
                    AbstractReplicationView
                            .getReplicationView(l, l.getReplicationMode(address), l.getSegment(address))
                            .fillStreamHole(streamID, address);
                    return null;
                }
        );
    }

    public void compactAll() {
        layoutHelper(l -> {
            for (Layout.LayoutSegment s : l.getSegments()) {
                for (Layout.LayoutStripe ls : s.getStripes()) {
                    for (String server : ls.getLogServers()) {
                        l.getRuntime().getRouter(server).getClient(LogUnitClient.class).forceCompact();
                    }
                }
            }
            return null;
        });
    }
}
