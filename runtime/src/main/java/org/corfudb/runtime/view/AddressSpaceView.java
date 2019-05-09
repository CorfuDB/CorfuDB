package org.corfudb.runtime.view;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.netty.handler.timeout.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IToken;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
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
import org.corfudb.util.CFUtils;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.Sleep;
import org.corfudb.util.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


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
    final LoadingCache<Long, ILogData> readCache = CacheBuilder.newBuilder()
            .maximumSize(runtime.getParameters().getNumCacheEntries())
            .expireAfterAccess(runtime.getParameters().getCacheExpiryTime(), TimeUnit.SECONDS)
            .expireAfterWrite(runtime.getParameters().getCacheExpiryTime(), TimeUnit.SECONDS)
            .recordStats()
            .build(new CacheLoader<Long, ILogData>() {
                @Override
                public ILogData load(Long value) throws Exception {
                    return fetch(value);
                }

                @Override
                public Map<Long, ILogData> loadAll(Iterable<? extends Long> keys) throws Exception {
                    return fetchAll((Iterable<Long>) keys, true);
                }
            });

    /**
     * Constructor for the Address Space View.
     */
    public AddressSpaceView(@Nonnull final CorfuRuntime runtime) {
        super(runtime);
        MetricRegistry metrics = CorfuRuntime.getDefaultMetrics();
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

    /**
     * Write the given log data using a token, returning
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
                log.error("write: Got exception during replication protocol write with token: {}", token, re);
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

    /**
     * Directly read from the log, returning any
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
            try {
                return readCache.get(address);
            } catch (ExecutionException | UncheckedExecutionException e) {
                // Guava wraps the exceptions thrown from the lower layers, therefore
                // we need to unwrap them before throwing them to the upper layers that
                // don't understand the guava exceptions
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else {
                    throw new RuntimeException(cause);
                }
            }
        }

        return fetch(address);
    }

    /**
     * This method reads a batch of addresses if 'nextRead' is not found in the cache.
     * In the case of a cache miss, it piggybacks on the read for nextRead.
     *
     * If 'nextRead' is present in the cache, it directly returns this data.
     *
     * @param nextRead current address of interest
     * @param addresses batch of addresses to read (bring into the cache) in case there is a cache miss (includes
     *                  nextRead)
     * @return data for current 'address' of interest.
     */
    public @Nonnull ILogData predictiveReadRange(Long nextRead, List<Long> addresses) {
        if (runtime.getParameters().isCacheDisabled()) {
            return fetch(nextRead);
        }

        ILogData data = readCache.getIfPresent(nextRead);
        if (data == null) {
            log.trace("predictiveReadRange: request to read {}", addresses);
            Map<Long, ILogData> mapAddresses = this.read(addresses, true);
            data = mapAddresses.get(nextRead);
        }

        return data;
    }

    /**
     * Read the given object from a range of addresses.
     *
     * - If the waitForWrite flag is set to true, when an empty address is encountered,
     * it waits for one hole to be filled. All the rest empty addresses within the list
     * are hole filled directly and the reader does not wait.
     * - In case the flag is set to false, none of the reads wait for write completion and
     * the empty addresses are hole filled right away.
     *
     * @param addresses An iterable with addresses to read from
     * @return A result, which be cached.
     */
    public Map<Long, ILogData> read(Iterable<Long> addresses, boolean waitForWrite) {
        if (!runtime.getParameters().isCacheDisabled() && waitForWrite) {
            try {
                return readCache.getAll(addresses);
            } catch (ExecutionException | UncheckedExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else {
                    throw new RuntimeException(cause);
                }
            }
        } else if (!waitForWrite) {
            return fetchAll(addresses, waitForWrite);
        }

        return fetchAll(addresses, true);
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
     * Get log address space, which includes:
     *     1. Addresses belonging to each stream.
     *     2. Log Tail.
     * @return
     */
    public StreamsAddressResponse getLogAddressSpace() {
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
                // By changing the order of the trimming operations, i.e., signal the
                // sequencer about a trim before actually trimming the log unit, we prevent a race condition,
                // in which a client could attempt to read a trimmed address over and over again, as signal
                // has not reached the sequencer. The problem with this is that we have a retry limit of 2, so
                // if the race is present on just one cycle we will abort due to trimmed exception.
                // In this case we avoid this case, and even if the log unit trim fails,
                // this data is checkpointed so there is no actual correctness implication.
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
                long runtimeEpoch = runtime.getLayoutView().getLayout().getEpoch();
                log.warn("prefixTrim[{}]: wrongEpochException, runtime is in epoch {}, while server is in epoch {}. "
                                + "Invalidate layout for this client and retry, attempt: {}/{}",
                        address, runtimeEpoch, serverEpoch, x + 1, numRetries);
                runtime.invalidateLayout();
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

    /**
     * Force all server caches to be invalidated.
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

    /**
     * Force the client cache to be invalidated.
     */
    public void invalidateClientCache() {
        readCache.invalidateAll();
    }

    /**
     * Fetch a collection of addresses for insertion into the cache.
     * The result map returned is ordered by address.
     *
     * @param addresses    collection of addresses to read from.
     * @param waitForWrite flag whether wait for write is required or hole fill directly.
     * @return a ordered map of read addresses.
     */
    @Nonnull
    public Map<Long, ILogData> fetchAll(Iterable<Long> addresses, boolean waitForWrite) {
        Map<Long, ILogData> result = new TreeMap<>();

        Iterable<List<Long>> batches = Iterables.partition(addresses,
                runtime.getParameters().getBulkReadSize());

        for (List<Long> batch : batches) {
            try {
                // Doesn't handle the case where some address have a different replication mode
                Map<Long, ILogData> batchResult = layoutHelper(e -> e.getLayout()
                        .getReplicationMode(batch.iterator().next())
                        .getReplicationProtocol(runtime)
                        .readAll(e, batch, waitForWrite));
                // Sanity check for returned addresses
                if (batchResult.size() != batch.size()) {
                    log.error("fetchAll: Requested number of addresses not equal to the read result" +
                            "from server, requested: {}, returned: {}", batch, batchResult.keySet());
                    throw new UnrecoverableCorfuError("Requested number of addresses not equal to the read result");
                }
                result.putAll(batchResult);
            } catch (Exception e) {
                log.error("fetchAll: Couldn't read addresses {}", batch, e);
                throw new UnrecoverableCorfuError("Unexpected error during fetchAll", e);
            }
        }

        result.forEach(this::checkLogData);
        return result;
    }

    /**
     * Checks whether a log entry is valid or not. If a read
     * returns null, Empty, or trimmed an exception will be
     * thrown.
     *
     * @param address the address being checked
     * @param logData the ILogData at the address being checked
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

    /**
     * Explicitly fetch a given address, bypassing the cache.
     *
     * @param address an address to read from.
     * @return the log data read at address
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
}
