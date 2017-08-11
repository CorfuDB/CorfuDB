package org.corfudb.runtime.view;

import com.codahale.metrics.Gauge;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Iterables;

import java.util.*;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.Getter;
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
import org.corfudb.util.CFUtils;


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
    @Getter
    final LoadingCache<Long, ILogData> readCache = Caffeine.<Long, ILogData>newBuilder()
            .maximumSize(runtime.getNumCacheEntries())
            .expireAfterAccess(runtime.getCacheExpiryTime(), TimeUnit.SECONDS)
            .expireAfterWrite(runtime.getCacheExpiryTime(), TimeUnit.SECONDS)
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

        final String pfx = String.format("%s0x%x.cache.", runtime.getMpASV(), this.hashCode());
        runtime.getMetrics().register(pfx + "cache-size",
                (Gauge<Long>) () -> readCache.estimatedSize());
        runtime.getMetrics().register(pfx + "evictions",
                (Gauge<Long>) () -> readCache.stats().evictionCount());
        runtime.getMetrics().register(pfx + "hit-rate",
                (Gauge<Double>) () -> readCache.stats().hitRate());
        runtime.getMetrics().register(pfx + "hits",
                (Gauge<Long>) () -> readCache.stats().hitCount());
        runtime.getMetrics().register(pfx + "misses",
                (Gauge<Long>) () -> readCache.stats().missCount());
    }

    /**
     * Reset all in-memory caches.
     */
    public void resetCaches() {
        readCache.invalidateAll();
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
        write(token, data, null);
    }

    public void write(IToken token, Object data, @Nullable AddressSpaceOptions opts)
            throws OverwriteException {
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
            l.getReplicationMode(token.getTokenValue())
                        .getReplicationProtocol(runtime)
                        .write(l, ld);
            return null;
        });

        // Cache the successful write
        if (!runtime.isCacheDisabled() && (opts == null || opts.isDoCache())) {
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
        return read(address, null);
    }

    public @Nonnull ILogData read(long address, AddressSpaceOptions opts) {
        ILogData data;
        if (!runtime.isCacheDisabled() && (opts == null || opts.isDoCache())) {
            data = readCache.get(address);
        } else {
            data = fetch(address);
        }

        if (data == null || data.getType() == DataType.EMPTY) {
            throw new RuntimeException("Unexpected return of empty data at address "
                    + address + " on read");
        } else if (data.isTrimmed()) {
            throw new TrimmedException();
        }
        return data;
    }

    /**
     * Read the given object from a set of addresses.
     *
     * @param addresses An iterable with addresses to read from
     * @param opts determine whether to cache the result
     * @return A result, which be cached.
     */
    public Map<Long, ILogData> read(Iterable<Long> addresses, AddressSpaceOptions opts) {
        Map<Long, ILogData> addressesMap;
        if (!runtime.isCacheDisabled() && (opts == null || opts.isDoCache()) ) {
            addressesMap = readCache.getAll(addresses);
        } else {
            addressesMap = cacheFetch(addresses);
        }

        for (ILogData logData : addressesMap.values()) {
            if (logData.isTrimmed()) {
                throw new TrimmedException();
            }
        }

        return addressesMap;
    }

    public Map<Long, ILogData> read(Iterable<Long> addresses) {
        return read(addresses, null);
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
            return;
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
     * Fetch a collection of addresses.
     *
     * @param addresses collection of addresses to read from.
     * @return A result to be cached
     */
    public @Nonnull
    Map<Long, ILogData> cacheFetch(Iterable<Long> addresses) {

        Map<Long, ILogData> allAddresses = new TreeMap<>();

        Iterable<List<Long>> batches = Iterables.partition(addresses, runtime.getBulkReadSize());

        for (List<Long> batch : batches) {
            try {
                //doesn't handle the case where some address have a different replication mode
                allAddresses.putAll(layoutHelper(l -> l.getReplicationMode(batch.iterator().next())
                        .getReplicationProtocol(runtime)
                        .readAll(l, batch)));
            } catch (Exception e) {
                log.error("cacheFetch: Couldn't read addresses {}", batch, e);
            }
        }

        return allAddresses;
    }

    /**
     * Fetch a collection of addresses.
     * TODO probably should be renamed bulkRead, since it is public
     *
     * @param addresses collection of addresses to read from.
     * @param options
     * @return A map of addresses -> log-entries
     */
    public @Nonnull
    Map<Long, ILogData> bulkFetchNoBatch(Set<Long> addresses, AddressSpaceOptions options) {
        return layoutHelper(l -> l.getReplicationMode(addresses.iterator().next())
                    .getReplicationProtocol(runtime)
                    .readRange(l, addresses));
    }

    /**
     * Explicitly fetch a given address.
     *
     * @param address An address to read from.
     * @return A log entry.
     */
    @Nonnull ILogData fetch(final long address) {
        return layoutHelper(l -> l.getReplicationMode(address)
                .getReplicationProtocol(runtime)
                .read(l, address)
        );
    }
}
