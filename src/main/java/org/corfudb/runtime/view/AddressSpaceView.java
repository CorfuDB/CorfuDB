package org.corfudb.runtime.view;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.util.CFUtils;
import sun.rmi.runtime.Log;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** A view of the address space implemented by Corfu.
 *
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class AddressSpaceView extends AbstractView {

    /** A cache for read results. */
    static LoadingCache<Long, AbstractReplicationView.ReadResult> readCache;

    /** Duration before retrying an empty read. */
    static final Duration emptyDuration = Duration.ofMillis(5000L);

    public AddressSpaceView(CorfuRuntime runtime)
    {
        super(runtime);
        // We don't lock readCache, this should be ok in the rare
        // case we generate a second readCache as it won't be pointed to.
        if (readCache == null) {
            readCache = Caffeine.newBuilder()
                    .maximumSize(10_000)
                    .build(this::cacheFetch);
        }
        else {
            log.debug("Read cache already built, re-using existing read cache.");
        }
    }

    /** Reset all in-memory caches. */
    public void resetCaches()
    {
        readCache.invalidateAll();
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
        layoutHelper(
          (LayoutFunction<Layout, Void, OverwriteException, RuntimeException, RuntimeException, RuntimeException>)
                l -> {
           AbstractReplicationView.getReplicationView(l, l.getReplicationMode(address))
                   .write(address, stream, data, backpointerMap);
           return null;
        });

        // Insert this write to our local cache.
        AbstractReplicationView.CachedLogUnitEntry cachedEntry =
                new AbstractReplicationView.CachedLogUnitEntry(LogUnitReadResponseMsg.ReadResultType.DATA,
                        data);
        cachedEntry.setBackpointerMap(backpointerMap);
        cachedEntry.setStreams(stream);
        readCache.put(address, new AbstractReplicationView.ReadResult(address, cachedEntry));
    }

    /**
     * Read the given object from an address and streams.
     *
     * @param address An address to read from.
     * @return        A result, which be cached.
     */
    public AbstractReplicationView.ReadResult read(long address)
    {
        return readCache.get(address);
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
                (LayoutFunction<Layout, Void, OverwriteException, RuntimeException, RuntimeException, RuntimeException>)
                l -> {AbstractReplicationView
                .getReplicationView(l, l.getReplicationMode(address))
                .fillHole(address);
                return null;}
        );
    }
}
