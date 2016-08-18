package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.corfudb.infrastructure.log.AbstractLocalLog;
import org.corfudb.infrastructure.log.InMemoryLog;
import org.corfudb.infrastructure.log.LogUnitEntry;
import org.corfudb.infrastructure.log.RollingLog;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuRangeMsg;
import org.corfudb.protocols.wireprotocol.LogUnitFillHoleMsg;
import org.corfudb.protocols.wireprotocol.LogUnitGCIntervalMsg;
import org.corfudb.protocols.wireprotocol.LogUnitReadRangeResponseMsg;
import org.corfudb.protocols.wireprotocol.LogUnitReadRequestMsg;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.ReadResultType;
import org.corfudb.protocols.wireprotocol.LogUnitTrimMsg;
import org.corfudb.protocols.wireprotocol.LogUnitWriteMsg;
import org.corfudb.util.Utils;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalAndSentinelRetry;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by mwei on 12/10/15.
 * <p>
 * A Log Unit Server, which is responsible for providing the persistent storage for the Corfu Distributed Shared Log.
 * <p>
 * All reads and writes go through a cache. If the sync flag (--sync) is set, the cache is configured in write-through
 * mode, otherwise the cache is configured in write-back mode. For persistence, every 10,000 log entries are written
 * to individual files (logs), which are represented as FileHandles. Each FileHandle contains a pointer to the tail
 * of the file, a memory-mapped file channel, and a set of addresses known to be in the file. To write an entry, the
 * pointer to the tail is first extended to the length of the entry, and the entry is added to the set of known
 * addresses. A header is written, which consists of the ASCII characters LE, followed by a set of flags,
 * the log unit address, the size of the entry, then the metadata size, metadata and finally the entry itself.
 * When the entry is complete, a written flag is set in the flags field.
 */
@Slf4j
public class LogUnitServer extends AbstractServer {

    /**
     * A scheduler, which is used to schedule periodic tasks like garbage collection.
     */
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("LogUnit-Maintenance-%d")
                            .build());
    /**
     * The options map.
     */
    Map<String, Object> opts;
    /**
     * The garbage collection thread.
     */
    Thread gcThread;
    /**
     * The contiguous head of the log (that is, the lowest address which has NOT been trimmed yet).
     */
    @Getter
    long contiguousHead;

    ConcurrentHashMap<UUID, Long> trimMap;
    IntervalAndSentinelRetry gcRetry;
    AtomicBoolean running = new AtomicBoolean(true);
    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent storage.
     */
    LoadingCache<Long, LogUnitEntry> dataCache;
    long maxCacheSize;

    private final AbstractLocalLog localLog;

    public LogUnitServer(ServerContext serverContext) {
        this.opts = serverContext.getServerConfig();

        maxCacheSize = Utils.parseLong(opts.get("--max-cache"));
        String logdir = opts.get("--log-path") + File.separator + "log";
        if ((Boolean) opts.get("--memory")) {
            log.warn("Log unit opened in-memory mode (Maximum size={}). " +
                    "This should be run for testing purposes only. " +
                    "If you exceed the maximum size of the unit, old entries will be AUTOMATICALLY trimmed. " +
                    "The unit WILL LOSE ALL DATA if it exits.", Utils.convertToByteStringRepresentation(maxCacheSize));
            localLog = new InMemoryLog(0, Long.MAX_VALUE);
            reset();
        } else {
            localLog = new RollingLog(0, Long.MAX_VALUE, logdir, (Boolean) opts.get("--sync"));
        }

        reset();

/*       compactTail seems to be broken, disabling it for now
         scheduler.scheduleAtFixedRate(this::compactTail,
                Utils.getOption(opts, "--compact", Long.class, 60L),
                Utils.getOption(opts, "--compact", Long.class, 60L),
                TimeUnit.SECONDS);*/

        gcThread = new Thread(this::runGC);
        gcThread.start();
    }

    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (isShutdown()) return;
        switch (msg.getMsgType()) {
            case WRITE:
                LogUnitWriteMsg writeMsg = (LogUnitWriteMsg) msg;
                log.trace("Handling write request for address {}", writeMsg.getAddress());
                write(writeMsg, ctx, r);
                break;
            case READ_REQUEST:
                LogUnitReadRequestMsg readMsg = (LogUnitReadRequestMsg) msg;
                log.trace("Handling read request for address {}", readMsg.getAddress());
                read(readMsg, ctx, r);
                break;
            case READ_RANGE:
                CorfuRangeMsg rangeReadMsg = (CorfuRangeMsg) msg;
                log.trace("Handling read request for address ranges {}", rangeReadMsg.getRanges());
                read(rangeReadMsg, ctx, r);
                break;
            case GC_INTERVAL: {
                LogUnitGCIntervalMsg m = (LogUnitGCIntervalMsg) msg;
                log.info("Garbage collection interval set to {}", m.getInterval());
                gcRetry.setRetryInterval(m.getInterval());
            }
            break;
            case FORCE_GC: {
                log.info("GC forced by client {}", msg.getClientID());
                gcThread.interrupt();
            }
            break;
            case FILL_HOLE: {
                LogUnitFillHoleMsg m = (LogUnitFillHoleMsg) msg;
                log.debug("Hole fill requested at {}", m.getAddress());
                dataCache.get(m.getAddress(), (address) -> new LogUnitEntry(address));
                r.sendResponse(ctx, m, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
            }
            break;
            case TRIM: {
                LogUnitTrimMsg m = (LogUnitTrimMsg) msg;
                trimMap.compute(m.getStreamID(), (key, prev) ->
                        prev == null ? m.getPrefix() : Math.max(prev, m.getPrefix()));
                log.debug("Trim requested at prefix={}", m.getPrefix());
            }
            break;
        }
    }

    @Override
    public void reset() {
        contiguousHead = 0L;

        if (dataCache != null) {
            /** Free all references */
            dataCache.asMap().values().parallelStream()
                    .map(m -> m.buffer.release());
        }

        dataCache = Caffeine.newBuilder()
                .<Long, LogUnitEntry>weigher((k, v) -> v.buffer == null ? 1 : v.buffer.readableBytes())
                .maximumWeight(maxCacheSize)
                .removalListener(this::handleEviction)
                .writer(new CacheWriter<Long, LogUnitEntry>() {
                    @Override
                    public void write(Long address, LogUnitEntry entry) {
                        if (dataCache.getIfPresent(address) != null) {// || seenAddresses.contains(address)) {
                            throw new RuntimeException("overwrite");
                        }
                        if (!entry.isPersisted) { //don't persist an entry twice.
                            localLog.write(address, entry);
                        }
                    }

                    @Override
                    public void delete(Long aLong, LogUnitEntry logUnitEntry, RemovalCause removalCause) {
                        // never need to delete
                    }
                }).build(this::handleRetrieval);

        // Trim map is set to empty on start
        // TODO: persist trim map - this is optional since trim is just a hint.
        trimMap = new ConcurrentHashMap<>();
    }

    /**
     * Retrieve the LogUnitEntry from disk, given an address.
     *
     * @param address The address to retrieve the entry from.
     * @return The log unit entry to retrieve into the cache.
     * This function should not care about trimmed addresses, as that is handled in
     * the read() and write(). Any address that cannot be retrieved should be returned as
     * unwritten (null).
     */
    public synchronized LogUnitEntry handleRetrieval(Long address) {
        LogUnitEntry entry = localLog.read(address);
        log.trace("Retrieved[{} : {}]", address, entry);
        return entry;
    }

    public synchronized void handleEviction(Long address, LogUnitEntry entry, RemovalCause cause) {
        log.trace("Eviction[{}]: {}", address, cause);
        if (entry.buffer != null) {
            // Free the internal buffer once the data has been evicted (in the case the server is not sync).
            entry.buffer.release();
        }
    }

    /**
     * Service an incoming read request.
     */
    public void read(LogUnitReadRequestMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("Read[{}]", msg.getAddress());
        LogUnitEntry e = dataCache.get(msg.getAddress());
        if (e == null) {
            r.sendResponse(ctx, msg, new LogUnitReadResponseMsg(ReadResultType.EMPTY));
        } else if (e.isHole) {
            r.sendResponse(ctx, msg, new LogUnitReadResponseMsg(ReadResultType.FILLED_HOLE));
        } else {
            r.sendResponse(ctx, msg, new LogUnitReadResponseMsg(e));
        }
    }

    /**
     * Service an incoming ranged read request.
     */
    public void read(CorfuRangeMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("ReadRange[{}]", msg.getRanges());
        Set<Long> total = new HashSet<>();
        for (Range<Long> range : msg.getRanges().asRanges()) {
            total.addAll(Utils.discretizeRange(range));
        }

        Map<Long, LogUnitEntry> e = dataCache.getAll(total);
        Map<Long, LogUnitReadResponseMsg> o = new ConcurrentHashMap<>();
        e.entrySet().parallelStream()
                .forEach(rv -> o.put(rv.getKey(), new LogUnitReadResponseMsg(rv.getValue())));
        r.sendResponse(ctx, msg, new LogUnitReadRangeResponseMsg(o));
    }

    /**
     * Service an incoming write request.
     */
    public void write(LogUnitWriteMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        long address = msg.getAddress();
        log.trace("Write[{}]", address);
        // The payload in the message is a view of a larger buffer allocated
        // by netty, thus direct memory can leak. Copy the view and release the
        // underlying buffer
        LogUnitEntry e = new LogUnitEntry(address, msg.getData().copy(), msg.getMetadataMap(), false);
        msg.getData().release();
        try {
            dataCache.put(e.getAddress(), e);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ERROR_OK));
        } catch (Exception ex) {
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ERROR_OVERWRITE));
            e.getBuffer().release();
        }
    }

    public void runGC() {
        Thread.currentThread().setName("LogUnit-GC");
        val retry = IRetry.build(IntervalAndSentinelRetry.class, this::handleGC)
                .setOptions(x -> x.setSentinelReference(running))
                .setOptions(x -> x.setRetryInterval(60_000));

        gcRetry = (IntervalAndSentinelRetry) retry;

        retry.runForever();
    }

    @SuppressWarnings("unchecked")
    public boolean handleGC() {
        log.info("Garbage collector starting...");
        long freedEntries = 0;

        /* Pick a non-compacted region or just scan the cache */
        Map<Long, LogUnitEntry> map = dataCache.asMap();
        SortedSet<Long> addresses = new TreeSet<>(map.keySet());
        for (long address : addresses) {
            LogUnitEntry buffer = dataCache.getIfPresent(address);
            if (buffer != null) {
                Set<UUID> streams = buffer.getStreams();
                // this is a normal entry
                if (streams.size() > 0) {
                    boolean trimmable = true;
                    for (java.util.UUID stream : streams) {
                        Long trimMark = trimMap.getOrDefault(stream, null);
                        // if the stream has not been trimmed, or has not been trimmed to this point
                        if (trimMark == null || address > trimMark) {
                            trimmable = false;
                            break;
                        }
                        // it is not trimmable.
                    }
                    if (trimmable) {
                        log.trace("Trimming entry at {}", address);
                        trimEntry(address, streams, buffer);
                        freedEntries++;
                    }
                } else {
                    //this is an entry which belongs in all streams
                }
            }
        }

        log.info("Garbage collection pass complete. Freed {} entries", freedEntries);
        return true;
    }

    public void trimEntry(long address, Set<java.util.UUID> streams, LogUnitEntry entry) {
        // Add this entry to the trimmed range map.
        //trimRange.add(Range.closed(address, address));
        // Invalidate this entry from the cache. This will cause the CacheLoader to free the entry from the disk
        // assuming the entry is back by disk
        dataCache.invalidate(address);
        //and free any references the buffer might have
        if (entry.getBuffer() != null) {
            entry.getBuffer().release();
        }
    }

    /**
     * Shutdown the server.
     */
    @Override
    public void shutdown() {
        scheduler.shutdownNow();
    }

    @VisibleForTesting
    LoadingCache<Long, LogUnitEntry> getDataCache() {
        return dataCache;
    }
}
