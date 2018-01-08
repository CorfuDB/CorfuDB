package org.corfudb.runtime.view;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IToken;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.StaleTokenException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.CFUtils;

import java.util.Comparator;



/**
 * A view of the address space implemented by Corfu.
 *
 * <p>Created by mwei on 12/10/15.</p>
 */
@Slf4j
public class AddressSpaceView extends AbstractView {

    /**
     * A cache for read results.
     */
    final LoadingCache<Long, ILogData> readCache = Caffeine.<Long, ILogData>newBuilder()
            .maximumSize(runtime.getParameters().getNumCacheEntries())
            .expireAfterAccess(runtime.getParameters().getCacheExpiryTime(), TimeUnit.SECONDS)
            .expireAfterWrite(runtime.getParameters().getCacheExpiryTime(), TimeUnit.SECONDS)
            .recordStats()
            .build(new CacheLoader<Long, ILogData>() {
                @Override
                public ILogData load(Long value) throws Exception {
                    return cacheFetch(value);
                }

                @Override
                public Map<Long, ILogData> loadAll(Iterable<? extends Long> keys) throws Exception {
                    return cacheFetch((Iterable<Long>) keys);
                }
            });

    /**
     * Constructor for the Address Space View.
     */
    public AddressSpaceView(@Nonnull final CorfuRuntime runtime) {
        super(runtime);
        MetricRegistry metrics = runtime.getMetrics();
        final String pfx = String.format("%s0x%x.cache.", runtime.getMpASV(), this.hashCode());
        metrics.register(pfx + "cache-size", (Gauge<Long>) readCache::estimatedSize);
        metrics.register(pfx + "evictions", (Gauge<Long>) () -> readCache.stats().evictionCount());
        metrics.register(pfx + "hit-rate", (Gauge<Double>) () -> readCache.stats().hitRate());
        metrics.register(pfx + "hits", (Gauge<Long>) () -> readCache.stats().hitCount());
        metrics.register(pfx + "misses", (Gauge<Long>) () -> readCache.stats().missCount());
    }


    /**
     * Reset all in-memory caches.
     */
    public void resetCaches() {
        readCache.invalidateAll();
    }


    /**
     * Validates the state of a write after an exception occured during the process
     *
     * There are [currently] three different scenarios:
     *   1. The data was persisted to some log units and we were able to recover it.
     *   2. The data was not persisted and another client (or ourself) hole filled.
     *      In that case, we return an OverwriteException and let the nex layer handle it.
     *   3. The address we tried to write to was trimmed. In this case, there is no way to
     *      know if the write went through or not. For sanity, we throw an OverwriteException
     *      and let the above layer retry.
     *
     * @param address
     */
    private void validateStateOfWrittenEntry(long address) {
        ILogData logData;
        try {
            logData = read(address);
        } catch (TrimmedException te) {
            // We cannot know if the write went through or not
            throw new UnrecoverableCorfuError("We cannot determine state of an update because of a trim.");
        }
        if (logData.isHole()) {
            throw new OverwriteException();
        }
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
    public void write(IToken token, Object data) throws OverwriteException {
        final ILogData ld = new LogData(DataType.DATA, data);

        layoutHelper(l -> {
            // Check if the token issued is in the same
            // epoch as the layout we are about to write
            // to.
            if (token.getEpoch() != l.getEpoch()) {
                throw new StaleTokenException(l.getEpoch());
            }

            // Set the data to use the token
            ld.useToken(token);


            // Do the write
            try {
                l.getReplicationMode(token.getTokenValue())
                        .getReplicationProtocol(runtime)
                        .write(l, ld);
            } catch (RuntimeException re) {
                // If we have an Overwrite exception, it is already too late for trying
                // to validate the state of the write, we know that it didn't went through.
                if (re instanceof OverwriteException) {
                    throw re;
                }

                validateStateOfWrittenEntry(token.getTokenValue());
            }
            return null;
        }, true);

        // Cache the successful write
        if (!runtime.getParameters().isCacheDisabled()) {
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
        if (!runtime.getParameters().isCacheDisabled()) {
            ILogData data = readCache.get(address);
            if (data == null || data.getType() == DataType.EMPTY) {
                throw new RuntimeException("Unexpected return of empty data at address "
                        + address + " on read");
            } else if (data.isTrimmed()) {
                throw new TrimmedException();
            }
            return data;
        }
        return fetch(address);
    }

    /**
     * Read the given object from a range of addresses.
     *
     * @param addresses An iterable with addresses to read from
     * @return A result, which be cached.
     */
    public Map<Long, ILogData> read(Iterable<Long> addresses) {
        Map<Long, ILogData> addressesMap;
        if (!runtime.getParameters().isCacheDisabled()) {
            addressesMap = readCache.getAll(addresses);
        } else {
            addressesMap = this.cacheFetch(addresses);
        }

        for (ILogData logData : addressesMap.values()) {
            if (logData.isTrimmed()) {
                throw new TrimmedException();
            }
        }

        return addressesMap;
    }

    /**
     * Get the first address in the address space.
     */
    public long getTrimMark() {
        return layoutHelper(l -> {
            return l.segments.stream()
                    .flatMap(seg -> seg.getStripes().stream())
                    .flatMap(stripe -> stripe.getLogServers().stream())
                    .map(endpoint ->
                            runtime.getRouter(endpoint)
                                    .getClient(LogUnitClient.class))
                    .map(LogUnitClient::getTrimMark)
                    .map(CFUtils::getUninterruptibly)
                    .max(Comparator.naturalOrder()).get();
        });
    }

    /**
     * Prefix trim the address space.
     *
     * <p>At the end of a prefix trim, all addresses equal to or
     * less than the address given will be marked for trimming,
     * which means that they may return either the original
     * data, or a trimmed exception.</p>
     *
     * @param address log address
     */
    public void prefixTrim(final long address) {
        log.debug("PrefixTrim[{}]", address);
        try {
            layoutHelper(l -> {
                        l.getPrefixSegments(address).stream()
                                .flatMap(seg -> seg.getStripes().stream())
                                .flatMap(stripe -> stripe.getLogServers().stream())
                                .map(endpoint ->
                                        runtime.getRouter(endpoint)
                                                .getClient(LogUnitClient.class))
                                .map(client -> client.prefixTrim(address))
                                .forEach(CFUtils::getUninterruptibly);
                        return null;    // No return value
                    }
            );

            runtime.getSequencerView().trimCache(address);

        } catch (Exception e) {
            log.error("prefixTrim: Error while calling prefix trimming {}", address, e);
            throw new UnrecoverableCorfuError("Unexpected error while prefix trimming", e);
        }
    }

    /** Force compaction on an address space, which will force
     * all log units to free space, and process any outstanding
     * trim requests.
     *
     */
    public void gc() {
        log.debug("GarbageCollect");
        layoutHelper(l -> {
            l.segments.stream()
                    .flatMap(seg -> seg.getStripes().stream())
                    .flatMap(stripe -> stripe.getLogServers().stream())
                    .map(endpoint ->
                            runtime.getRouter(endpoint)
                                    .getClient(LogUnitClient.class))
                    .map(LogUnitClient::compact)
                    .forEach(CFUtils::getUninterruptibly);
            return null;
        });
    }

    /** Force all server caches to be invalidated.
     */
    public void invalidateServerCaches() {
        log.debug("InvalidateServerCaches");
        layoutHelper(l -> {
            l.segments.stream()
                    .flatMap(seg -> seg.getStripes().stream())
                    .flatMap(stripe -> stripe.getLogServers().stream())
                    .map(endpoint ->
                            runtime.getRouter(endpoint)
                                    .getClient(LogUnitClient.class))
                    .map(LogUnitClient::flushCache)
                    .forEach(CFUtils::getUninterruptibly);
            return null;
        });
    }

    /** Force the client cache to be invalidated. */
    public void invalidateClientCache() {
        readCache.invalidateAll();
    }

    /**
     * Fetch an address for insertion into the cache.
     *
     * @param address An address to read from.
     * @return A result to be cached. If the readresult is empty,
     *         This entry will be scheduled to self invalidate.
     */
    private @Nonnull ILogData cacheFetch(long address) {
        log.trace("CacheMiss[{}]", address);
        ILogData result = fetch(address);
        if (result.getType() == DataType.EMPTY) {
            throw new RuntimeException("Unexpected empty return at " +  address + " from fetch");
        }
        return result;
    }

    /**
     * Fetch a collection of addresses for insertion into the cache.
     *
     * @param addresses collection of addresses to read from.
     * @return A result to be cached
     */
    public @Nonnull
    Map<Long, ILogData> cacheFetch(Iterable<Long> addresses) {
        Map<Long, ILogData> allAddresses = new HashMap<>();

        Iterable<List<Long>> batches = Iterables.partition(addresses,
            runtime.getParameters().getBulkReadSize());

        for (List<Long> batch : batches) {
            try {
                //doesn't handle the case where some address have a different replication mode
                allAddresses.putAll(layoutHelper(l -> l.getReplicationMode(batch.iterator().next())
                        .getReplicationProtocol(runtime)
                        .readAll(l, batch)));
            } catch (Exception e) {
                log.error("cacheFetch: Couldn't read addresses {}", batch, e);
                throw new UnrecoverableCorfuError(
                    "Unexpected error during cacheFetch", e);
            }
        }

        return allAddresses;
    }

    /**
     * Fetch a collection of addresses.
     *
     * @param addresses collection of addresses to read from.
     * @return A result to be cached
     */
    public @Nonnull
    Map<Long, ILogData> cacheFetch(Set<Long> addresses) {
        return layoutHelper(l -> l.getReplicationMode(addresses.iterator().next())
                .getReplicationProtocol(runtime)
                .readRange(l, addresses));
    }


    /**
     * Explicitly fetch a given address, bypassing the cache.
     *
     * @param address An address to read from.
     * @return A result, which will be uncached.
     */
    public @Nonnull ILogData fetch(final long address) {
        return layoutHelper(l -> l.getReplicationMode(address)
                .getReplicationProtocol(runtime)
                .read(l, address)
        );
    }
}
