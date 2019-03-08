package org.corfudb.runtime.view;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.handler.timeout.TimeoutException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.HashSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IToken;
import org.corfudb.protocols.wireprotocol.LogAddressSpaceResponse;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.AddressLoader;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.StaleTokenException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.CFUtils;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.Sleep;
import org.corfudb.util.Utils;


/**
 * A view of the address space implemented by Corfu.
 *
 * <p>Created by mwei on 12/10/15.</p>
 */
@Slf4j
public class AddressSpaceView extends AbstractView {

    private boolean useAddressLoader = true;

    /**
     * A cache for read results.
     */
    final Cache<Long, ILogData> readCache = CacheBuilder.newBuilder()
            .maximumSize(runtime.getParameters().getNumCacheEntries())
            .expireAfterAccess(runtime.getParameters().getCacheExpiryTime(), TimeUnit.SECONDS)
            .expireAfterWrite(runtime.getParameters().getCacheExpiryTime(), TimeUnit.SECONDS)
            .recordStats()
            .removalListener(new RemovalListener<Long, ILogData>() {
                @Override
                public void onRemoval(RemovalNotification<Long, ILogData> removal) {
                    log.trace("Removing entry for address {} from cache due to: {} ", removal.getKey(), removal.getCause().toString());
                }
            })
            .build();

     /**
      * To dispatch tasks for failure or healed nodes detection.
      */

     @Getter
     private final ExecutorService addressLoaderWorker;

     AddressLoader loader;

    /**
     * Constructor for the Address Space View.
     */
    public AddressSpaceView(@Nonnull final CorfuRuntime runtime) {
        super(runtime);
        MetricRegistry metrics = CorfuRuntime.getDefaultMetrics();

        this.loader = new AddressLoader(this::cacheFetch, readCache);

        // Initialize background thread running AddressLoader.fetch / dispatcher
        // Creating the detection worker thread pool.
        // This thread pool is utilized to dispatch detection tasks at regular intervals in the
        // detectorTaskScheduler.
        this.addressLoaderWorker = Executors.newFixedThreadPool(
                1,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("batcher-%d")
                        .build()
        );

        this.addressLoaderWorker.submit(loader);

        final String pfx = String.format("%s0x%x.cache.", CorfuComponent.ADDRESS_SPACE_VIEW.toString(),
                this.hashCode());
        metrics.register(pfx + "cache-size", (Gauge<Long>) readCache::size);
        metrics.register(pfx + "evictions", (Gauge<Long>) () -> readCache.stats().evictionCount());
        metrics.register(pfx + "hit-rate", (Gauge<Double>) () -> readCache.stats().hitRate());
        metrics.register(pfx + "hits", (Gauge<Long>) () -> readCache.stats().hitCount());
        metrics.register(pfx + "misses", (Gauge<Long>) () -> readCache.stats().missCount());
    }


    /**
     * Remove all log entries that are less than the trim mark
     */
    public void gc(long trimMark) {
        readCache.asMap().entrySet().removeIf(e -> e.getKey() < trimMark);
    }

    /**
     * Reset all in-memory caches.
     */
    public void resetCaches() {
        readCache.invalidateAll();
    }

    /**
     * Validates the state of a write after an exception occurred during the process
     *
     * There are [currently] three different scenarios:
     *   1. The data was persisted to some log units and we were able to recover it.
     *   2. The data was not persisted and another client (or this client) hole filled.
     *      In that case, we return an OverwriteException and let the upper layer handle it.
     *   3. The address we tried to write to was trimmed. In this case, there is no way to
     *      know if the write went through or not. For sanity, we throw an OverwriteException
     *      and let the above layer retry.
     *
     * @param address
     */
    private void validateStateOfWrittenEntry(long address, @Nonnull ILogData ld) {
        ILogData logData;
        try {
            logData = read(address);
        } catch (TrimmedException te) {
            // We cannot know if the write went through or not
            throw new UnrecoverableCorfuError("We cannot determine state of an update because of a trim.");
        }

        if (!logData.equals(ld)){
            throw new OverwriteException(OverwriteCause.DIFF_DATA);
        }
    }

    /** Write the given log data using a token, returning
     * either when the write has been completed successfully,
     * or throwing an OverwriteException if another value
     * has been adopted, or a WrongEpochException if the
     * token epoch is invalid.
     *
     * @param token        The token to use for the write.
     * @param data         The data to write.
     * @param cacheOption  The caching behaviour for this write
     * @throws OverwriteException   If the globalAddress given
     *                              by the token has adopted
     *                              another value.
     * @throws WrongEpochException  If the token epoch is invalid.
     */
    public void write(@Nonnull IToken token, @Nonnull Object data, @Nonnull CacheOption cacheOption) {
        ILogData ld;
        if (data instanceof ILogData) {
            ld = (ILogData) data;
        } else {
            ld = new LogData(DataType.DATA, data);
        }

        layoutHelper(e -> {
            Layout l = e.getLayout();
            // Check if the token issued is in the same
            // epoch as the layout we are about to write
            // to.
            if (token.getEpoch() != l.getEpoch()) {
                throw new StaleTokenException(l.getEpoch());
            }

            // Set the data to use the token
            ld.useToken(token);
            ld.setId(runtime.getParameters().getClientId());


            // Do the write
            try {
                l.getReplicationMode(token.getSequence())
                            .getReplicationProtocol(runtime)
                            .write(e, ld);
            } catch (OverwriteException ex) {
                if (ex.getOverWriteCause() == OverwriteCause.SAME_DATA){
                    // If we have an overwrite exception with the SAME_DATA cause, it means that the
                    // server suspects our data has already been written, in this case we need to
                    // validate the state of the write.
                    validateStateOfWrittenEntry(token.getSequence(), ld);
                } else {
                    // If we have an Overwrite exception with a different cause than SAME_DATA
                    // we do not need to validate the state of the write, as we know we have been
                    // certainly overwritten either by other data, by a hole or the address was trimmed.
                    // Large writes are also rejected right away.
                    throw ex;
                }
            } catch (WriteSizeException we) {
                throw we;
            } catch (RuntimeException re) {
                validateStateOfWrittenEntry(token.getSequence(), ld);
            }
            return null;
        }, true);

        // Cache the successful write
        if (!runtime.getParameters().isCacheDisabled() && cacheOption == CacheOption.WRITE_THROUGH) {
            readCache.put(token.getSequence(), ld);
        }
    }

    /**
     * Write the given log data and then add it to the address
     * space cache (i.e. WRITE_THROUGH option)
     *
     * @see AddressSpaceView#write(IToken, Object, CacheOption)
     */
    public void write(IToken token, Object data) throws OverwriteException {
        write(token, data, CacheOption.WRITE_THROUGH);
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
        return layoutHelper(e -> e.getLayout().getReplicationMode(address)
                    .getReplicationProtocol(runtime)
                    .peek(e, address));
    }

    /**
     * Read the given object from an address and streams.
     *
     * @param address An address to read from.
     * @return A result, which be cached.
     */
    public @Nonnull
    ILogData read(long address) {
        if (!runtime.getParameters().isCacheDisabled()) {
            // The VersionLockedObject and the Transaction layer will generate
            // undoRecord(s) during a transaction commit, or object sync. These
            // undo records are stored in transient fields and are not persisted.
            // A missing undo record can cause a NoRollbackException, thus forcing
            // a complete object rebuild that generates a "scanning" behavior
            // which affects the LRU window. In essence, affecting other cache users
            // and making the VersionLockedObject very sensitive to caching behavior.
            // A concrete example of this would be unsynchronized readers/writes:
            // 1. Thread A starts replicating write1
            // 2. Thread B discovers the write (via stream tail query) and
            //    tries to read write1
            // 3. Thread B's read results in a cache miss and the reader thread
            //    starts loading the value into the cache
            // 4. Thread A completes its write and caches it with undo records
            // 5. Thread B finishes loading and caches the loaded value replacing
            //    the cached value from step 4 (i.e. loss of undo records computed
            //    by thread A)
            ILogData data = readCache.getIfPresent(address);
            if (data == null) {
                // Loading a value without the cache loader can result in
                // redundant loading calls (i.e. multiple threads try to
                // load the same value), but currently a redundant RPC
                // is much cheaper than the cost of a NoRollBackException, therefore
                // this trade-off is reasonable

                // No Address Loader
                if (!useAddressLoader) {
                    final ILogData loadedVal = fetch(address);
                    data = readCache.asMap().computeIfAbsent(address, (k) -> loadedVal);
                } else {
                    // Address Loader
                    CompletableFuture<Map<Long, ILogData>> cf = loader.enqueueReads(new ArrayList<>(Arrays.asList(address)));
                    Map<Long, ILogData> loadedVal = waitOnReads(cf);
                    data = readCache.asMap().computeIfAbsent(address, (k) -> loadedVal.get(address));
                }
            }

            return data;
        }

        return fetch(address);
    }

    /**
     * Read a range of addresses in a predictive way.
     *
     * If there is a cache miss, read a range of addresses, otherwise
     * directly return the requested entry and read is defer.
     */
    public @Nonnull ILogData predictiveReadRange(Long address, List<Long> addresses) {
        if (!runtime.getParameters().isCacheDisabled()) {
            log.trace("predictiveReadRange: request to read {}", addresses);
            ILogData data = readCache.getIfPresent(address);
            if (data == null) {
                Map<Long, ILogData> mapAddresses = this.read(addresses);
                data = mapAddresses.get(address);
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
        Map<Long, ILogData> result = new HashMap<>();

        if (!runtime.getParameters().isCacheDisabled()) {
            Set<Long> addressesToFetch = new HashSet<>();

            for (Long address : addresses) {
                ILogData val = readCache.getIfPresent(address);
                if (val == null) {
                    addressesToFetch.add(address);
                } else {
                    result.put(address, val);
                }
            }

            // At this point we computed a subset of the addresses that
            // resulted in a cache miss and need to be fetched
            if (!addressesToFetch.isEmpty()) {
                Map<Long, ILogData> fetchedAddresses;

                if (!useAddressLoader) {
                    // No Address Loader
                    fetchedAddresses = cacheFetch(addressesToFetch);

                    for (Long address : fetchedAddresses.keySet()) {
                        checkLogData(address, fetchedAddresses.get(address));
                    }
                } else {
                    // Address Loader
                    CompletableFuture<Map<Long, ILogData>> cf = loader.enqueueReads(Lists.newArrayList(addressesToFetch));
                    fetchedAddresses = waitOnReads(cf);
                }

                for (Map.Entry<Long, ILogData> entry : fetchedAddresses.entrySet()) {
                    // After fetching a value, we need to insert it in the cache.
                    // Note that based on code inspection it seems like operations
                    // on the cache's map view are reflected in the cache's statistics.
                    result.put(entry.getKey(), readCache.asMap()
                            .computeIfAbsent(entry.getKey(), (k) -> entry.getValue()));
                }
            }
        } else {
            CompletableFuture<Map<Long, ILogData>> cf = loader.enqueueReads(Lists.newArrayList(addresses));
            result.putAll(waitOnReads(cf));
        }

        return result;
    }

    private Map<Long, ILogData> waitOnReads(CompletableFuture<Map<Long, ILogData>> cf) {
        try {
            Map<Long, ILogData> fetchedAddresses = cf.get();

            for (Long address : fetchedAddresses.keySet()) {
                checkLogData(address, fetchedAddresses.get(address));
            }

            return fetchedAddresses;
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        } catch (ExecutionException ee) {
            throw new RuntimeException(ee);
        }
    }

    /**
     * Get the first address in the address space.
     */
    public Token getTrimMark() {
        return layoutHelper(
                e -> {
                    long trimMark = e.getLayout().segments.stream()
                            .flatMap(seg -> seg.getStripes().stream())
                            .flatMap(stripe -> stripe.getLogServers().stream())
                            .map(e::getLogUnitClient)
                            .map(LogUnitClient::getTrimMark)
                            .map(future -> {
                                // This doesn't look nice, but its required to trigger
                                // the retry mechanism in AbstractView. Also, getUninterruptibly
                                // can't be used here because it throws a UnrecoverableCorfuInterruptedError
                                try {
                                    return future.join();
                                } catch (CompletionException ex) {
                                    Throwable cause = ex.getCause();
                                    if (cause instanceof RuntimeException) {
                                        throw (RuntimeException) cause;
                                    } else {
                                        throw new RuntimeException(cause);
                                    }
                                }
                            })
                            .max(Comparator.naturalOrder()).get();
                    return new Token(e.getLayout().getEpoch(), trimMark);
                });
    }

    /**
     * Get the log's tail, i.e., last address in the address space.
     */
    public Long getLogTail() {
        return layoutHelper(
                e -> Utils.getLogTail(e.getLayout(), runtime));
    }

    /**
     * Get all tails, includes: log tail and stream tails.
     */
    public TailsResponse getAllTails() {
        return layoutHelper(
                e -> Utils.getAllTails(e.getLayout(), runtime));
    }

    /**
     * Get all streams address space.
     */
    public StreamsAddressResponse getStreamsAddressSpace() {
        return layoutHelper(
                e -> Utils.getStreamsAddressSpace(e.getLayout(), runtime));
    }

    /**
     * Get log address space, which includes:
     *     1. All streams address space.
     *     2. Log Tail.
     * @return
     */
    public LogAddressSpaceResponse getLogAddressSpace() {
        return layoutHelper(
                e -> Utils.getLogAddressSpace(e.getLayout(), runtime));
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
    public void prefixTrim(final Token address) {
        log.info("PrefixTrim[{}]", address);
        final int numRetries = 3;

        for (int x = 0; x < numRetries; x++) {
            try {
                // TODO(Maithem): trimCache should be epoch aware?
                runtime.getSequencerView().trimCache(address.getSequence());
                layoutHelper(e -> {
                            e.getLayout().getPrefixSegments(address.getSequence()).stream()
                                    .flatMap(seg -> seg.getStripes().stream())
                                    .flatMap(stripe -> stripe.getLogServers().stream())
                                    .map(e::getLogUnitClient)
                                    .map(client -> client.prefixTrim(address))
                                    .forEach(cf -> {CFUtils.getUninterruptibly(cf,
                                            NetworkException.class, TimeoutException.class,
                                            WrongEpochException.class);
                                    });
                            return null;
                }, true);
                break;
            } catch (NetworkException | TimeoutException e) {
                log.warn("prefixTrim: encountered a network error on try {}", x, e);
                Duration retryRate = runtime.getParameters().getConnectionRetryRate();
                Sleep.sleepUninterruptibly(retryRate);
            } catch (WrongEpochException wee) {
                long serverEpoch = wee.getCorrectEpoch();
                // Retry if wrongEpochException corresponds to message epoch (only)
                if (address.getEpoch() == serverEpoch) {
                    long runtimeEpoch = runtime.getLayoutView().getLayout().getEpoch();
                    log.warn("prefixTrim[{}]: wrongEpochException, runtime is in epoch {}, " +
                            "while server is in epoch {}. Invalidate layout for this client " +
                            "and retry, attempt: {}/{}", address, runtimeEpoch, serverEpoch, x+1, numRetries);
                    runtime.invalidateLayout();
                } else {
                    // wrongEpochException corresponds to a stale trim address (prefix trim token on the wrong epoch)
                    log.error("prefixTrim[{}]: stale prefix trim. Prefix trim on wrong epoch {}, " +
                            "while server on epoch {}", address, address.getEpoch(), serverEpoch);
                    throw wee;
                }
            }
        }
    }

    /** Force compaction on an address space, which will force
     * all log units to free space, and process any outstanding
     * trim requests.
     *
     */
    public void gc() {
        log.debug("GarbageCollect");
        layoutHelper(e -> {
            e.getLayout().segments.stream()
                    .flatMap(seg -> seg.getStripes().stream())
                    .flatMap(stripe -> stripe.getLogServers().stream())
                    .map(e::getLogUnitClient)
                    .map(LogUnitClient::compact)
                    .forEach(CFUtils::getUninterruptibly);
            return null;
        });
    }

    /** Force all server caches to be invalidated.
     */
    public void invalidateServerCaches() {
        log.debug("InvalidateServerCaches");
        layoutHelper(e -> {
            e.getLayout().segments.stream()
                    .flatMap(seg -> seg.getStripes().stream())
                    .flatMap(stripe -> stripe.getLogServers().stream())
                    .map(e::getLogUnitClient)
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
     * Fetch a collection of addresses for insertion into the cache.
     *
     * @param addresses collection of addresses to read from.
     * @return A result to be cached
     */
    public @Nonnull
    Map<Long, ILogData> fetchAll(Iterable<Long> addresses) {
        Map<Long, ILogData> result = new HashMap<>();

        Iterable<List<Long>> batches = Iterables.partition(addresses,
                runtime.getParameters().getBulkReadSize());

        for (List<Long> batch : batches) {
            try {
                //doesn't handle the case where some address have a different replication mode
                result.putAll(layoutHelper(e -> e.getLayout()
                        .getReplicationMode(batch.iterator().next())
                        .getReplicationProtocol(runtime)
                        .readAll(e, batch)));
            } catch (Exception e) {
                log.error("cacheFetch: Couldn't read addresses {}", batch, e);
                throw new UnrecoverableCorfuError(
                        "Unexpected error during cacheFetch", e);
            }
        }

        for (Long address : result.keySet()) {
            checkLogData(address, result.get(address));
        }
        return result;
    }

    /**
     * Fetch a range of addresses.
     *
     * @param addresses collection of addresses to read from.
     * @return A result to be cached
     */
    public @Nonnull
    Map<Long, ILogData> fetchRange(Set<Long> addresses) {
        Map<Long, ILogData> result = layoutHelper(e -> e.getLayout().getReplicationMode(addresses.iterator().next())
                .getReplicationProtocol(runtime)
                .readRange(e, addresses));

        for (Long address : result.keySet()) {
            checkLogData(address, result.get(address));
        }
        return result;
    }

    /**
     * Checks whether a log entry is valid or not. If a read
     * returns null, Empty, or trimmed an exception will be
     * thrown.
     * @param address The address being checked
     * @param logData the IlogData at the address being checked
     */
    private void checkLogData(long address, ILogData logData) {
        if (logData == null || logData.getType() == DataType.EMPTY) {
            throw new RuntimeException("Unexpected return of empty data at address "
                    + address + " on read");
        }

        if (logData.isTrimmed()) {
            throw new TrimmedException();
        }
    }

    public @Nonnull Map<Long, ILogData> cacheFetch(Set<Long> addresses) {
        return layoutHelper(e -> e.getLayout().getReplicationMode(addresses.iterator().next())
                .getReplicationProtocol(runtime)
                .multiRead(e, new ArrayList<>(addresses), true));

        // No checkLogData is required because this cacheFetch is used by lower layers, AddressLoader,
        // TrimmedExceptions will be handled by upper layers (calling into the read)
    }

    /**
     * Explicitly fetch a given address, bypassing the cache.
     *
     * @param address An address to read from.
     * @return A result, which will be uncached.
     */
    public @Nonnull
    ILogData fetch(final long address) {
        ILogData result = layoutHelper(e -> e.getLayout().getReplicationMode(address)
                .getReplicationProtocol(runtime)
                .read(e, address)
        );

        checkLogData(address, result);
        return result;
    }

    @VisibleForTesting
    Cache<Long, ILogData> getReadCache() {
        return readCache;
    }

    public void stop() {
        this.loader.stop();
        this.addressLoaderWorker.shutdownNow();
    }
}
