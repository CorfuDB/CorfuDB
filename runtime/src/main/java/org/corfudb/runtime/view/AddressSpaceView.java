package org.corfudb.runtime.view;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.netty.handler.timeout.TimeoutException;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMRGarbageEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IToken;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.QuotaExceededException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.StaleTokenException;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.CFUtils;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.Sleep;
import org.corfudb.util.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


/**
 * A view of the address space implemented by Corfu.
 *
 * <p>Created by mwei on 12/10/15.</p>
 */
@Slf4j
public class AddressSpaceView extends AbstractView {

    private final static long CACHE_KEY_SIZE = MetricsUtils.sizeOf.deepSizeOf(0L);
    private final static long DEFAULT_MAX_CACHE_ENTRIES = 5000;

    /**
     * A cache for read results.
     */
    private final Cache<Long, ILogData> readCache;

    static final private Duration GC_INTERVAL = Duration.ofMinutes(30);

    @Getter
    @Setter
    private AtomicLong compactionMark = new AtomicLong(Address.NON_ADDRESS);

    private volatile long lastTrimMark = Address.NON_ADDRESS;

    private volatile long lastTrimTime = -1L;

    private final ReadOptions defaultReadOptions = ReadOptions.builder()
            .waitForHole(true)
            .clientCacheable(true)
            .serverCacheable(true)
            .build();

    /**
     * Constructor for the Address Space View.
     */
    public AddressSpaceView(@Nonnull final CorfuRuntime runtime) {
        super(runtime);

        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();

        final boolean cacheDisabled = runtime.getParameters().isCacheDisabled();
        final long maxCacheEntries = runtime.getParameters().getMaxCacheEntries();
        final long maxCacheWeight = runtime.getParameters().getMaxCacheWeight();
        final int concurrencyLevel = runtime.getParameters().getCacheConcurrencyLevel();

        if (maxCacheWeight != 0) {
            cacheBuilder.maximumWeight(maxCacheWeight);
            cacheBuilder.weigher((k, v) -> (int) (CACHE_KEY_SIZE + MetricsUtils.sizeOf.deepSizeOf(v)));
        }

        if (cacheDisabled) {
            cacheBuilder.maximumSize(0); // Do not allocate memory when cache is disabled.
        } else if (maxCacheEntries != 0) {
            cacheBuilder.maximumSize(maxCacheEntries);
        } else if (maxCacheWeight == 0) {
            // If cache weight/size are not set, then we default to using size based cache.
            cacheBuilder.maximumSize(DEFAULT_MAX_CACHE_ENTRIES);
        }

        if (concurrencyLevel != 0) {
            cacheBuilder.concurrencyLevel(concurrencyLevel);
        }

        readCache = cacheBuilder.expireAfterAccess(runtime.getParameters().getCacheExpiryTime(), TimeUnit.SECONDS)
                .expireAfterWrite(runtime.getParameters().getCacheExpiryTime(), TimeUnit.SECONDS)
                .removalListener(this::handleEviction)
                .recordStats()
                .build();

        MetricRegistry metrics = CorfuRuntime.getDefaultMetrics();
        final String pfx = String.format("%s0x%x.cache.", CorfuComponent.ADDRESS_SPACE_VIEW.toString(),
                                         this.hashCode());
        metrics.register(pfx + "cache-size", (Gauge<Long>) readCache::size);
        metrics.register(pfx + "evictions", (Gauge<Long>) () -> readCache.stats().evictionCount());
        metrics.register(pfx + "hit-rate", (Gauge<Double>) () -> readCache.stats().hitRate());
        metrics.register(pfx + "hits", (Gauge<Long>) () -> readCache.stats().hitCount());
        metrics.register(pfx + "misses", (Gauge<Long>) () -> readCache.stats().missCount());
    }

    private void handleEviction(RemovalNotification<Long, ILogData> notification) {
        if (log.isTraceEnabled()) {
            log.trace("handleEviction: evicting {} cause {}", notification.getKey(), notification.getCause());
        }
    }

    /**
     * Reset all in-memory caches.
     */
    public void resetCaches() {
        readCache.invalidateAll();
    }

    /**
     * Remove all log entries that are less than the trim mark
     */
    public void gc(long trimMark) {
        readCache.asMap().entrySet().removeIf(e -> e.getKey() < trimMark);
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
        ILogData logData = read(address);

        if (!logData.equals(ld)){
            throw new OverwriteException(OverwriteCause.DIFF_DATA);
        }
    }

    /**
     * Sends garbage decisions to LogUnit servers in a stripe.
     * @param runtimeLayout Corfu runtime layout.
     * @param stripeIndex  the index of one stripe in layout.
     * @param garbageEntries a collection of garbageEntries which contain garbage decisions.
     */
    public void writeGarbage(RuntimeLayout runtimeLayout, int stripeIndex,
                             Collection<SMRGarbageEntry> garbageEntries) {
        List<LogData> garbageDataList =
                garbageEntries.stream().map(garbageEntry -> {

            // epoch in token is not used, therefore it is set to a dummy value -1.
            Token token = new Token(-1, garbageEntry.getGlobalAddress());
            LogData garbageData = new LogData(DataType.GARBAGE, garbageEntry);
            garbageData.useToken(token);

            return garbageData;
        }).collect(Collectors.toList());

        List<Layout.LayoutSegment> segments = runtimeLayout.getLayout().getSegments();

        // TODO(xin): explain
        // get the servers from the last segment
        List<String> servers = segments.get(segments.size() - 1).getStripes().get(stripeIndex).getLogServers();
        int numUnits = servers.size();

        CompletableFuture[] futures = new CompletableFuture[numUnits];

        for (int i = 0; i < numUnits; ++i) {
            log.trace("writing garbage [{}]: {}/{}", servers, i + 1, numUnits);
            String server = servers.get(i);
            futures[i] = CompletableFuture.runAsync(() -> writeGarbageToLogUnit(runtimeLayout, server,
                    garbageDataList));
        }

        CFUtils.getUninterruptibly(CompletableFuture.allOf(futures));

        log.trace("writeGarbage complete[{}]", servers);
    }

    private void writeGarbageToLogUnit(RuntimeLayout runtimeLayout, String logUnitServer,
                                       List<LogData> garbageEntries) {
        LogUnitClient client = runtimeLayout.getLogUnitClient(logUnitServer);
        garbageEntries.forEach(garbageEntry -> garbageEntry.setId(runtime.getParameters().getClientId()));

        final int numRetries = 3;

        for (int x = 0; x < numRetries; x++) {
            try {
                CFUtils.getUninterruptibly(client.writeGarbageEntries(garbageEntries),
                        NetworkException.class, TimeoutException.class, WrongEpochException.class);
                return;
            } catch (NetworkException | TimeoutException e) {
                log.warn("writeGarbageToLogUnit: accessing LogUnit {} encounters a network error. " +
                                "Invalidate layout for this client and retry, attempt: {}/{}",
                        logUnitServer, x + 1, numRetries, e);
                Duration retryRate = runtime.getParameters().getConnectionRetryRate();
                Sleep.sleepUninterruptibly(retryRate);
            } catch (WrongEpochException wee) {
                long serverEpoch = wee.getCorrectEpoch();
                long runtimeEpoch = runtime.getLayoutView().getLayout().getEpoch();
                log.warn("writeGarbageToLogUnit: wrongEpochException, runtime is in epoch {}, " +
                                "while server is in epoch {}. Invalidate layout for this client and retry, attempt: " +
                                "{}/{}", runtimeEpoch, serverEpoch, x + 1, numRetries);
                runtime.invalidateLayout();
            }
        }

        String errorMsg = String.format("Writing garbage to %s fails after %d attempts",
                logUnitServer, numRetries);

        log.warn(errorMsg);
        throw new RetryExhaustedException(errorMsg);
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
            ld = new LogData(DataType.DATA, data, runtime.getParameters().getCodecType());
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
            } catch (WriteSizeException | QuotaExceededException ie) {
                log.warn("write: write failed", ie);
                throw ie;
            } catch (RuntimeException re) {
                log.error("write: Got exception during replication protocol write with token: {}", token, re);
                validateStateOfWrittenEntry(token.getSequence(), ld);
            }
            return null;
        }, true);

        // Cache the successful write
        if (cacheOption == CacheOption.WRITE_THROUGH) {
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
     * Commit the addresses in the range by first inspecting the addresses
     * and if data does not exist in log, hole fill the address. This is
     * used by management agent for log consolidation.
     *
     * @param start start of address range, inclusive
     * @param end   end of address range, inclusive
     */
    public void commit(long start, long end) {
        if (start >= end) {
            return;
        }

        ContiguousSet<Long> range = ContiguousSet.create(
                Range.closed(start, end), DiscreteDomain.longs());

        // Commit the addresses and update all log unit servers with the
        // new committed tail, which is the end of range. Exceptions are
        // handled at the upper layer.
        layoutHelper(e -> {
            e.getLayout().getReplicationMode(start)
                    .getReplicationProtocol(runtime)
                    .commitAll(e, range);
            Utils.updateCommittedTail(e.getLayout(), runtime, end);
            return null;
        }, true);
    }

    /**
     * Perform a single read with the default read options
     */
    public @NonNull ILogData read(long address) {
        return read(address, defaultReadOptions);
    }

    /**
     * Read the given object from an address and streams.
     *
     * @param address An address to read from.
     * @param options Read options for this particular write (i.e. configure caching behavior)
     * @return A result, which be cached.
     */
    public @Nonnull
    ILogData read(long address, @NonNull ReadOptions options) {
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
            final ILogData loadedVal = fetch(address);
            return cacheLoadAndGet(readCache, address, loadedVal, options);
        }
        return data;
    }

    /**
     * This method reads a batch of addresses if 'nextRead' is not found in the cache.
     * In the case of a cache miss, it piggybacks on the read for nextRead.
     *
     * If 'nextRead' is present in the cache, it directly returns this data.
     *
     * @param nextRead current address of interest
     * @param addresses batch of addresses to read (bring into the cache) in case
     *                  there is a cache miss (includes nextRead)
     * @param options  options for this read request
     * @return data for current 'address' of interest.
     */
    public @Nonnull
    ILogData read(@NonNull Long nextRead, @NonNull NavigableSet<Long> addresses,
                  @NonNull ReadOptions options) {
        if (options.isClientCacheable()) {
            ILogData data = readCache.getIfPresent(nextRead);
            if (data == null) {
                List<Long> batch = getBatch(nextRead, addresses);
                log.trace("read: request address {}, read batch {}", nextRead, batch);
                Map<Long, ILogData> mapAddresses = read(batch, options);
                data = mapAddresses.getOrDefault(nextRead, LogData.getCompacted(nextRead));
            }

            return data;
        }

        return fetch(nextRead);
    }

    /**
     * Prepare a batch of entries to be read, including the current address to retrieve.
     *
     * @param currentRead current address to retrieve.
     * @param queue queue to get entries from.
     * @return batch of entries.
     */
    private List<Long> getBatch(long currentRead, @NonNull NavigableSet<Long> queue) {
        List<Long> batchRead = new ArrayList<>();
        batchRead.add(currentRead);

        Iterator<Long> it = queue.iterator();
        while (it.hasNext() && batchRead.size() < runtime.getParameters().getStreamBatchSize()) {
            batchRead.add(it.next());
        }

        return batchRead;
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
    public Map<Long, ILogData> read(Iterable<Long> addresses) {
        return read(addresses, defaultReadOptions);
    }


    /**
     * Attempts to insert a loaded value into the cache and return the cached value for a particular key.
     */
    private ILogData cacheLoadAndGet(@NonNull Cache<Long, ILogData> cache, long address,
                                     @NonNull ILogData loadedValue,
                                     @NonNull ReadOptions options) {

        if (!options.isClientCacheable()) {
            return loadedValue;
        }

        try {
            return cache.get(address, () -> loadedValue);
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

    /**
     * Read the given object from a range of addresses.
     *
     * @param addresses An iterable with addresses to read from
     * @return A map of addresses read, which will be cached if caching is enabled
     */
    public Map<Long, ILogData> read(Iterable<Long> addresses, @NonNull ReadOptions options) {
        final Map<Long, ILogData> cachedData = readCache.getAllPresent(addresses);
        final Set<Long> addressesToFetch = Sets.difference(
                Sets.newHashSet(addresses), cachedData.keySet());

        final Map<Long, ILogData> uncachedData = fetchAll(addressesToFetch, options);
        final List<Long> compactedAddresses = filterCompactedAddresses(uncachedData);
        compactedAddresses.forEach(uncachedData::remove);

        final Map<Long, ILogData> result = uncachedData.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry ->
                        cacheLoadAndGet(readCache, entry.getKey(), entry.getValue(), options)));
        result.putAll(cachedData);

        return result;
    }

    /**
     * Get the log's tail, i.e., last address in the address space.
     */
    public Long getLogTail() {
        return layoutHelper(
                e -> Utils.getLogTail(e.getLayout(), runtime));
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
        // No-op, only for temporary backward compatibility.
    }

    /**
     * Get the first address in the address space.
     */
    public Token getTrimMark() {
        // No-op, only for temporary backward compatibility.
        return new Token(-1, -1);
    }

    /**
     * Force compaction on an address space, which will force
     * all log units to free space, and process any outstanding
     * trim requests.
     */
    public void gc() {
        // No-op, only for temporary backward compatibility.
    }

    /**
     * Get all tails, includes: log tail and stream tails.
     */
    public TailsResponse getAllTails() {
        return layoutHelper(
                e -> Utils.getAllTails(e.getLayout(), runtime));
    }

    /**
     * Get the maximum committed log tail from all log units.
     */
    public long getCommittedTail() {
        return layoutHelper(
                e -> Utils.getCommittedTail(e.getLayout(), runtime), true);
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
     *
     * Does not validate log entries.
     *
     * @param addresses     collection of addresses to read from.
     *
     * @return a map of read addresses.
     */
    @Nonnull
    private Map<Long, ILogData> fetchAll(Iterable<Long> addresses, ReadOptions options) {
        final Map<Long, ILogData> rawData = new HashMap<>();
        if (Iterables.isEmpty(addresses)) {
            return rawData;
        }

        final Iterable<List<Long>> batches = Iterables.partition(addresses,
                runtime.getParameters().getBulkReadSize());

        for (List<Long> batch : batches) {
            // Doesn't handle the case where some address have a different replication mode
            Map<Long, ILogData> batchResult = layoutHelper(layout -> layout.getLayout()
                    .getReplicationMode(batch.iterator().next())
                    .getReplicationProtocol(runtime)
                    .readAll(layout, batch, options.isWaitForHole(), options.isServerCacheable()));
            // Sanity check for returned addresses
            if (batchResult.size() != batch.size()) {
                log.error("fetchAll: Requested number of addresses not equal to the read result" +
                        "from server, requested: {}, returned: {}", batch, batchResult.keySet());
                throw new IllegalStateException("Requested number of addresses not equal to the read result");
            }
            rawData.putAll(batchResult);
        }

        return rawData;
    }

    /**
     * Given the input data, deduce which addresses have been compacted.
     *
     * @param allData data on which the operation will be performed
     * @return the list of addresses that have been compacted
     */
    private List<Long> filterCompactedAddresses(Map<Long, ILogData> allData) {
        return allData.entrySet().stream()
                .filter(entry -> !isLogDataValid(entry.getKey(), entry.getValue()))
                .map(Entry::getKey).collect(Collectors.toList());
    }

    /**
     * Checks whether a log entry is valid and un-compacted. If a read
     * result is null or Empty, an exception will be thrown, if a read
     * result is a compacted entry, return false.
     *
     * @param address the address being checked
     * @param logData the ILogData at the address being checked
     * @return true if valid data, false if address is compacted.
     */
    private boolean isLogDataValid(long address, ILogData logData) {
        if (logData == null || logData.getType() == DataType.EMPTY) {
            throw new RuntimeException("Unexpected return of empty data at address "
                    + address + " on read");
        }

        return !logData.isCompacted();
    }

    /**
     * Explicitly fetch a given address, bypassing the cache.
     *
     * @param address an address to read from.
     * @return the log data read at address
     */
    private  @Nonnull
    ILogData fetch(final long address) {
        ILogData result = layoutHelper(e -> e.getLayout().getReplicationMode(address)
                .getReplicationProtocol(runtime)
                .read(e, address)
        );

        isLogDataValid(address, result);

        return result;
    }

    /**
     * Fetch garbage decisions from LogUnit servers.
     *
     * @param addresses an iterator of a collection of addresses to read.
     * @return garbage decisions.
     */
    @Nonnull
    public Map<Long, LogData> FetchGarbageEntries(Iterable<Long> addresses) {
        Iterable<List<Long>> batches = Iterables.partition(addresses,
                runtime.getParameters().getBulkReadSize());

        // The garbage should come from the same log unit servers.
        // Prevent batch results come from different log unit due to layout change. 
        return layoutHelper(e -> {
            Map<Long, LogData> garbageEntries = new TreeMap<>();
            for (List<Long> batch : batches) {
                Map<Long, LogData> batchResult = FetchGarbageEntryBatch(batch, e);
                garbageEntries.putAll(batchResult);
            }
            return garbageEntries;
        });
    }

    @Nonnull
    private Map<Long, LogData> FetchGarbageEntryBatch(Iterable<Long> addresses, RuntimeLayout runtimeLayout) {
        Map<String, List<Long>> serverToAddressMap = new HashMap<>();
        for (Long address : addresses) {
            List<String> logServers = runtimeLayout.getLayout().getStripe(address).getLogServers();
            String logServer = logServers.get(logServers.size() - 1);
            List<Long> addressList = serverToAddressMap.computeIfAbsent(logServer, s -> new ArrayList<>());
            addressList.add(address);
        }

        // Send read requests to log unit servers in parallel
        List<CompletableFuture<ReadResponse>> futures = serverToAddressMap.entrySet().stream()
                .map(entry -> runtimeLayout.getLogUnitClient(entry.getKey()).readGarbageEntries(entry.getValue()))
                .collect(Collectors.toList());

        // Merge the read responses from different log unit servers
        return futures.stream()
                .map(future -> CFUtils.getUninterruptibly(future).getAddresses())
                .reduce(new HashMap<>(), (map1, map2) -> {
                    map1.putAll(map2);
                    return map1;
                });
    }

    @VisibleForTesting
    Cache<Long, ILogData> getReadCache() {
        return readCache;
    }

    /**
     * Update compaction mark in the AddressSpaceView.
     * Compaction mark could only monotonically grow.
     * @param runtime         corfu runtime
     * @param compactionMark  compaction mark
     */
    public void updateCompactionMark(CorfuRuntime runtime, long compactionMark) {
        // Compaction mark could only monotonically grow.
        runtime.getAddressSpaceView().getCompactionMark().getAndUpdate((address) -> {
            if (address < compactionMark) {
                log.trace("Compaction mark is updated to {}", compactionMark);

                // garbage collection is trigger only when compaction mark grows, therefore
                // triggerGC would NOT be invoked too frequently.
                triggerGC();

                return compactionMark;
            } else {
                return address;
            }
        });
    }

    private void triggerGC() {
        if (Address.nonAddress(lastTrimMark))  {
            lastTrimMark = compactionMark.get();
            lastTrimTime = System.currentTimeMillis();
        } else {
            long ts = System.currentTimeMillis();

            // The interval between two consecutive garbage collections should be greater
            // than a cool down time
            if (ts - lastTrimTime > GC_INTERVAL.toMillis()) {

                // garbage collection is deferred for a cycle to prevent evict information
                // which would be used shortly.
                runtime.getGarbageCollector().gc(lastTrimMark);
                lastTrimTime = ts;
                lastTrimMark = compactionMark.get();
            }
        }
    }

}
