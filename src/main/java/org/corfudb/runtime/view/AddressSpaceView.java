package org.corfudb.runtime.view;

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
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.Serializers;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

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
    static LoadingCache<Long, LogData> readCache;

    /**
     * Duration before retrying an empty read.
     */
    @Getter
    @Setter
    Duration emptyDuration = Duration.ofMillis(5000L);

    public AddressSpaceView(CorfuRuntime runtime) {
        super(runtime);
        // We don't lock readCache, this should be ok in the rare
        // case we generate a second readCache as it won't be pointed to.
        if (readCache == null) {
            resetCaches();
        } else {
            log.debug("Read cache already built, re-using existing read cache.");
        }
    }

    /**
     * Reset all in-memory caches.
     */
    public void resetCaches() {
        readCache = Caffeine.<Long, LogData>newBuilder()
                .<Long, LogData>weigher((k, v) -> v.getType() != DataType.DATA ? 1 : v.getData().readableBytes())
                .maximumWeight(runtime.getMaxCacheSize())
                .build(new CacheLoader<Long, LogData>() {
                    @Override
                    public LogData load(Long aLong) throws Exception {
                        return cacheFetch(aLong);
                    }

                    @Override
                    public Map<Long, LogData> loadAll(Iterable<? extends Long> keys) throws Exception {
                        return cacheFetch((Iterable<Long>) keys);
                    }
                });
    }

    /**
     * Learn about a stream for the first time.
     * This method will dump all learned stream entries into the stream.
     *
     * @param streamID The ID of a stream.
     * @return The long
     */
    private Set<Long> getStream(UUID streamID) {
        /* TODO : implement in both backpointer and Replex cases */
        throw new UnsupportedOperationException("unsupported");
    }

    /**
     * Learn about a stream for the first time, bypassing the cache.
     * This method will dump all learned stream entries into the stream.
     *
     * @param streamID The ID of a stream.
     * @return The long
     */
    private Map<Long, ILogUnitEntry> fetchStream(UUID streamID) {
        /* TODO : implement in both backpointer and Replex cases */
        throw new UnsupportedOperationException("unsupported");
    }

    /**
     * Write the given object to an address and streams.
     *
     * @param address        An address to write to.
     * @param stream         The streams which will belong on this entry.
     * @param data           The data to write.
     * @param backpointerMap
     */
    public void write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap)
            throws OverwriteException {
        int numBytes = layoutHelper(l -> AbstractReplicationView.getReplicationView(l, l.getReplicationMode(address),
                l.getSegment(address))
                .write(address, stream, data, backpointerMap));

        // Insert this write to our local cache.
        if (!runtime.isCacheDisabled()) {
            //TODO: fix me
            ByteBuf b = PooledByteBufAllocator.DEFAULT.buffer();
            Serializers.getSerializer(Serializers.SerializerType.CORFU)
                    .serialize(data, b);

            LogData ld = new LogData(DataType.DATA, b);
            ld.setGlobalAddress(address);
            ld.setBackpointerMap(backpointerMap);
            ld.setStreams(stream);

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
    public LogData read(long address) {
        if (!runtime.isCacheDisabled()) {
            return readCache.get(address);
        }
        return fetch(address);
    }

    /**
     * Read the given object from a range of addresses.
     *
     * @param addresses An address range to read from.
     * @return A result, which be cached.
     */
    public Map<Long, LogData> read(RangeSet<Long> addresses) {

        if (!runtime.isCacheDisabled()) {
            return readCache.getAll(Utils.discretizeRangeSet(addresses));
        }
        return this.cacheFetch(Utils.discretizeRangeSet(addresses));
    }

    /**
     * Read the given object from a range of addresses.
     *
     * @param stream An address range to read from.
     * @return A result, which be cached.
     */
    public Map<Long, LogData> readPrefix(UUID stream) {
        /* TODO : implement in both backpointer and Replex cases */
        throw new UnsupportedOperationException("unsupported");
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
            //schedule an eviction
            CompletableFuture.runAsync(() -> {
                log.trace("Evicting empty entry at {}.", address);
                CFUtils.runAfter(emptyDuration, () -> {
                    readCache.invalidate(address);
                });
            });
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
    private Map<Long, LogData> cacheFetch(Iterable<Long> addresses) {
        // for each address, figure out which replication group it goes to.
        Map<AbstractReplicationView, RangeSet<Long>> groupMap = new ConcurrentHashMap<>();
        return layoutHelper(l -> {
                    for (Long a : addresses) {
                        AbstractReplicationView v = AbstractReplicationView
                                .getReplicationView(l, l.getReplicationMode(a), l.getSegment(a));
                        groupMap.computeIfAbsent(v, x -> TreeRangeSet.<Long>create())
                                .add(Range.singleton(a));
                    }
                    Map<Long, LogData> result =
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
