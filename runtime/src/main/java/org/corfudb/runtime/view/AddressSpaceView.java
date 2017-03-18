package org.corfudb.runtime.view;

import com.codahale.metrics.Gauge;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.InMemoryLogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.Serializers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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

    /**
     * Write the given object to an address and streams.
     *
     * @param address        An address to write to.
     * @param stream         The streams which will belong on this entry.
     * @param data           The data to write.
     * @param backpointerMap
     */
    public void write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap,
                      Map<UUID, Long> streamAddresses)
            throws OverwriteException {
        write(address, stream, data, backpointerMap, streamAddresses, null);
    }

    public void write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap,
                      Map<UUID, Long> streamAddresses, Function<UUID, Object> partialEntryFunction)
            throws OverwriteException {
        int numBytes = layoutHelper(l -> AbstractReplicationView.getReplicationView(l, l.getReplicationMode(address),
                l.getSegment(address))
                .write(address, stream, data, backpointerMap, streamAddresses, partialEntryFunction));

        // Insert this append to our local cache.
        if (!runtime.isCacheDisabled()) {
            InMemoryLogData ld = new InMemoryLogData(DataType.DATA, data);
            ld.setGlobalAddress(address);
            ld.setBackpointerMap(backpointerMap);
            ld.setStreams(stream);
            ld.setLogicalAddresses(streamAddresses);

            // FIXME
            if (data instanceof LogEntry) {
                ((LogEntry) data).setRuntime(runtime);
                ((LogEntry) data).setEntry(ld);
            }
            readCache.put(address, ld);
        }
    }

    /**
     * Read the given object from an address and streams.
     *
     * @param address An address to read from.
     * @return A result, which be cached.
     */
    public ILogData read(long address) {
        if (!runtime.isCacheDisabled()) {
            ILogData data = readCache.get(address);
            if (data == null) {
                data = new InMemoryLogData(DataType.EMPTY);
                data.setGlobalAddress(address);
            }
            return data;
        }
        return fetch(address);
    }

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
    private LogData cacheFetch(long address) {
        log.trace("Cache miss @ {}, fetching.", address);
        LogData result = fetch(address);
        if (result.getType() == DataType.EMPTY) {
            return null;
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
        // for each address, figure out which replication group it goes to.
        Map<AbstractReplicationView, RangeSet<Long>> groupMap = new ConcurrentHashMap<>();
        return layoutHelper(l -> {
                    for (Long a : addresses) {
                        AbstractReplicationView v = AbstractReplicationView
                                .getReplicationView(l, l.getReplicationMode(a), l.getSegment(a));
                        groupMap.computeIfAbsent(v, x -> TreeRangeSet.<Long>create())
                                .add(Range.singleton(a));
                    }
                    Map<Long, ILogData> result =
                            new ConcurrentHashMap<>();
                    for (AbstractReplicationView vk : groupMap.keySet()) {
                        result.putAll(vk.read(groupMap.get(vk)));
                    }
                    return result;
                }
        );
    }


    /**
     * Explicitly fetch a given address, bypassing the cache.
     *
     * @param address An address to read from.
     * @return A result, which will be uncached.
     */
    public LogData fetch(long address) {
        return layoutHelper(l -> AbstractReplicationView
                .getReplicationView(l, l.getReplicationMode(address), l.getSegment(address))
                .read(address)
        );
    }

    /**
     * Fill a hole at the given address.
     *
     * @param address An address to hole fill at.
     */
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
