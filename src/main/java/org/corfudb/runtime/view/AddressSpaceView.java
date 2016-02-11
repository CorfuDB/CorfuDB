package org.corfudb.runtime.view;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Utils;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/** A view of the address space implemented by Corfu.
 *
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class AddressSpaceView extends AbstractView {

    /** A cache for read results. */
    static LoadingCache<Long, AbstractReplicationView.ReadResult> readCache;

    /** Duration before retrying an empty read. */
    @Getter
    @Setter
    Duration emptyDuration = Duration.ofMillis(5000L);

    public AddressSpaceView(CorfuRuntime runtime)
    {
        super(runtime);
        // We don't lock readCache, this should be ok in the rare
        // case we generate a second readCache as it won't be pointed to.
        if (readCache == null) {
            resetCaches();
        }
        else {
            log.debug("Read cache already built, re-using existing read cache.");
        }
    }

    /** Reset all in-memory caches. */
    public void resetCaches()
    {
        readCache = Caffeine.<Long, AbstractReplicationView.ReadResult>newBuilder()
                .<Long, AbstractReplicationView.ReadResult>weigher((k,v) -> v.result.getSizeEstimate())
                .maximumWeight(runtime.getMaxCacheSize())
                .build(new CacheLoader<Long, AbstractReplicationView.ReadResult>() {
                    @Override
                    public AbstractReplicationView.ReadResult load(Long aLong) throws Exception {
                        return cacheFetch(aLong);
                    }

                    @Override
                    public Map<Long, AbstractReplicationView.ReadResult> loadAll(Iterable<? extends Long> keys) throws Exception {
                        return cacheFetch((Iterable)keys);
                    }
                });
    }

    /**
     * Write the given object to an address and streams.
     *
     * @param address           An address to write to.
     * @param stream        The streams which will belong on this entry.
     * @param data          The data to write.
     * @param backpointerMap
     */
    public void write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap)
    throws OverwriteException
    {
        int numBytes = layoutHelper(l -> AbstractReplicationView.getReplicationView(l, l.getReplicationMode(address))
                   .write(address, stream, data, backpointerMap));

        // Insert this write to our local cache.
        if (!runtime.isCacheDisabled()) {
            AbstractReplicationView.CachedLogUnitEntry cachedEntry =
                    new AbstractReplicationView.CachedLogUnitEntry(LogUnitReadResponseMsg.ReadResultType.DATA,
                            data, numBytes);
            cachedEntry.setBackpointerMap(backpointerMap);
            cachedEntry.setStreams(stream);
            readCache.put(address, new AbstractReplicationView.ReadResult(address, cachedEntry));
        }
    }

    /**
     * Read the given object from an address and streams.
     *
     * @param address An address to read from.
     * @return        A result, which be cached.
     */
    public AbstractReplicationView.ReadResult read(long address)
    {
        if (!runtime.isCacheDisabled()) {
            return readCache.get(address);
        }
        return fetch(address);
    }

    /**
     * Read the given object from a range of addresses.
     *
     * @param addresses An address range to read from.
     * @return        A result, which be cached.
     */
    public Map<Long, AbstractReplicationView.ReadResult> read(RangeSet<Long> addresses)
    {

        if (!runtime.isCacheDisabled()) {
            return readCache.getAll(Utils.discretizeRangeSet(addresses));
        }
        return this.cacheFetch(Utils.discretizeRangeSet(addresses));
    }

    /**
     * Fetch an address for insertion into the cache.
     * @param address An address to read from.
     * @return        A result to be cached. If the readresult is empty,
     *                This entry will be scheduled to self invalidate.
     */
    private AbstractReplicationView.ReadResult cacheFetch(long address)
    {
        log.trace("Cache miss @ {}, fetching.", address);
        AbstractReplicationView.ReadResult result = fetch(address);
        if (result.getResult().getResultType() == LogUnitReadResponseMsg.ReadResultType.EMPTY)
        {
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
     * @param addresses An address to read from.
     * @return        A result to be cached. If the readresult is empty,
     *                This entry will be scheduled to self invalidate.
     */
    private Map<Long, AbstractReplicationView.ReadResult> cacheFetch(Iterable<Long> addresses)
    {
        // for each address, figure out which replication group it goes to.
        Map<AbstractReplicationView, RangeSet<Long>> groupMap = new ConcurrentHashMap<>();
        return layoutHelper(l -> {
                    for (Long a : addresses) {
                        AbstractReplicationView v = AbstractReplicationView
                                .getReplicationView(l, l.getReplicationMode(a));
                        groupMap.computeIfAbsent(v, x -> TreeRangeSet.<Long>create())
                                .add(Range.singleton(a));
                    }
                    Map<Long, AbstractReplicationView.ReadResult> result =
                            new ConcurrentHashMap<Long, AbstractReplicationView.ReadResult>();
                    for (AbstractReplicationView vk : groupMap.keySet())
                    {
                        result.putAll(vk.read(groupMap.get(vk)));
                    }
                    return result;
                }
        );
    }


    /**
     * Explicitly fetch a given address, bypassing the cache.
     * @param address An address to read from.
     * @return        A result, which will be uncached.
     */
    public AbstractReplicationView.ReadResult fetch(long address)
    {
        return layoutHelper(l -> AbstractReplicationView
                        .getReplicationView(l, l.getReplicationMode(address))
                        .read(address)
        );
    }

    /**
     * Fill a hole at the given address.
     * @param address An address to hole fill at.
     */
    public void fillHole(long address)
    throws OverwriteException
    {
        layoutHelper(
                l -> {AbstractReplicationView
                .getReplicationView(l, l.getReplicationMode(address))
                .fillHole(address);
                return null;}
        );
    }
}
