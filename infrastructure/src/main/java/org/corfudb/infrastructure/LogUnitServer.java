package org.corfudb.infrastructure;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;


import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.channel.ChannelHandlerContext;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import org.corfudb.infrastructure.log.InMemoryStreamLog;
import org.corfudb.infrastructure.log.LogAddress;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.CommitRequest;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadRequest;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.TrimRequest;
import org.corfudb.protocols.wireprotocol.WriteMode;
import org.corfudb.protocols.wireprotocol.WriteRequest;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.Utils;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalAndSentinelRetry;

import static org.corfudb.infrastructure.ServerContext.SMALL_INTERVAL;

/**
 * Created by mwei on 12/10/15.
 * <p>
 * A Log Unit Server, which is responsible for providing the persistent storage for the Corfu Distributed Shared Log.
 * <p>
 * All reads and writes go through a cache. For persistence, every 10,000 log entries are written to individual
 * files (logs), which are represented as FileHandles. Each FileHandle contains a pointer to the tail of the file, a
 * memory-mapped file channel, and a set of addresses known to be in the file. To write an entry, the pointer to the
 * tail is first extended to the length of the entry, and the entry is added to the set of known addresses. A header
 * is written, which consists of the ASCII characters LE, followed by a set of flags, the log unit address, the size
 * of the entry, then the metadata size, metadata and finally the entry itself. When the entry is complete, a written
 * flag is set in the flags field.
 */
@Slf4j
public class LogUnitServer extends AbstractServer {

    private final ServerContext serverContext;

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
     * GC parameters
     * TODO: entire GC handling needs updating, currently not being activated
     */
    private final Thread gcThread = null;
    private IntervalAndSentinelRetry gcRetry;
    private AtomicBoolean running = new AtomicBoolean(true);

    /**
     * The options map.
     */
    private final Map<String, Object> opts;

    /**
     * Handler for the base server
     */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .generateHandlers(MethodHandles.lookup(), this);

    private final ConcurrentHashMap<UUID, Long> trimMap;

    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent storage.
     */
    private final LoadingCache<LogAddress, LogData> dataCache;
    private final long maxCacheSize;

    private final StreamLog streamLog;

    private final BatchWriter<LogAddress, LogData> batchWriter;

    public LogUnitServer(ServerContext serverContext) {
        this.opts = serverContext.getServerConfig();
        this.serverContext = serverContext;
        maxCacheSize = Utils.parseLong(opts.get("--max-cache"));

        if ((Boolean) opts.get("--memory")) {
            log.warn("Log unit opened in-memory mode (Maximum size={}). " +
                    "This should be run for testing purposes only. " +
                    "If you exceed the maximum size of the unit, old entries will be AUTOMATICALLY trimmed. " +
                    "The unit WILL LOSE ALL DATA if it exits.", Utils.convertToByteStringRepresentation(maxCacheSize));
            streamLog = new InMemoryStreamLog();
        } else {
            String logdir = opts.get("--log-path") + File.separator + "log";
            File dir = new File(logdir);
            if (!dir.exists()) {
                dir.mkdir();
            }
            streamLog = new StreamLogFiles(logdir, (Boolean) opts.get("--no-verify"));
        }

        batchWriter = new BatchWriter(streamLog);

        dataCache = Caffeine.<LogAddress, LogData>newBuilder()
                .<LogAddress, LogData>weigher((k, v) -> v.getData() == null ? 1 : v.getData().length)
                .maximumWeight(maxCacheSize)
                .removalListener(this::handleEviction)
                .writer(batchWriter)
                .build(this::handleRetrieval);

        // Trim map is set to empty on start
        // TODO: persist trim map - this is optional since trim is just a hint.
        trimMap = new ConcurrentHashMap<>();
    }

    /**
     * Service an incoming write request.
     */
    @ServerHandler(type = CorfuMsgType.WRITE)
    public void write(CorfuPayloadMsg<WriteRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.debug("log write: global: {}, streams: {}, backpointers: {}", msg.getPayload().getGlobalAddress(),
                msg.getPayload().getStreamAddresses(), msg.getPayload().getData().getBackpointerMap());
        // clear any commit record (or set initially to false).
        msg.getPayload().clearCommit();
        try {
            if (msg.getPayload().getWriteMode() != WriteMode.REPLEX_STREAM) {
                dataCache.put(new LogAddress(msg.getPayload().getGlobalAddress(), null), msg.getPayload().getData());
                r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());
                return;
            } else {
                for (UUID streamID : msg.getPayload().getStreamAddresses().keySet()) {
                    dataCache.put(new LogAddress(msg.getPayload().getStreamAddresses().get(streamID), streamID),
                            msg.getPayload().getData());
                }
                r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());
            }
        } catch (OverwriteException ex) {
            if (msg.getPayload().getWriteMode() != WriteMode.REPLEX_STREAM)
                r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE.msg());
            else
                r.sendResponse(ctx, msg, CorfuMsgType.ERROR_REPLEX_OVERWRITE.msg());
        }
    }

    /**
     * Service an incoming commit request.
     */
    @ServerHandler(type = CorfuMsgType.COMMIT)
    public void commit(CorfuPayloadMsg<CommitRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        Map<UUID, Long> streamAddresses = msg.getPayload().getStreams();
        if (streamAddresses == null) {
            // Then this is a commit bit for the global log.
            LogData entry = dataCache.get(new LogAddress(msg.getPayload().getAddress(), null));
            if (entry == null) {
                r.sendResponse(ctx, msg, CorfuMsgType.ERROR_NOENTRY.msg());
                return;
            } else {
                entry.getMetadataMap().put(IMetadata.LogUnitMetadataType.COMMIT, msg.getPayload().getCommit());
            }
        } else {
            for (UUID streamID : msg.getPayload().getStreams().keySet()) {
                LogData entry = dataCache.get(new LogAddress(streamAddresses.get(streamID), streamID));
                if (entry == null) {
                    r.sendResponse(ctx, msg, CorfuMsgType.ERROR_NOENTRY.msg());
                    // TODO: Crap, we have to go back and undo all the commit bits??
                    return;
                } else {
                    entry.getMetadataMap().put(IMetadata.LogUnitMetadataType.COMMIT, msg.getPayload().getCommit());
                }
            }
        }
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type = CorfuMsgType.READ_REQUEST)
    private void read(CorfuPayloadMsg<ReadRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.debug("log read: {} {}", msg.getPayload().getStreamID(), msg.getPayload().getRange());
        ReadResponse rr = new ReadResponse();
        try {
            for (Long l = msg.getPayload().getRange().lowerEndpoint();
                 l < msg.getPayload().getRange().upperEndpoint() + 1L; l++) {
                LogAddress logAddress = new LogAddress(l, msg.getPayload().getStreamID());
                LogData e = dataCache.get(logAddress);
                if (e == null) {
                    rr.put(l, LogData.EMPTY);
                } else if (e.getType() == DataType.HOLE) {
                    rr.put(l, LogData.HOLE);
                } else {
                    rr.put(l, e);
                }
            }
            r.sendResponse(ctx, msg, CorfuMsgType.READ_RESPONSE.payloadMsg(rr));
        } catch (DataCorruptionException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_CORRUPTION.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.GC_INTERVAL)
    private void setGcInterval(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx, IServerRouter r) {
        gcRetry.setRetryInterval(msg.getPayload());
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type = CorfuMsgType.FORCE_GC)
    private void forceGc(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        gcThread.interrupt();
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type = CorfuMsgType.FILL_HOLE)
    private void fillHole(CorfuPayloadMsg<TrimRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        try {
            dataCache.put(new LogAddress(msg.getPayload().getPrefix(), msg.getPayload().getStream()), LogData.HOLE);
            r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());

        } catch (OverwriteException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.TRIM)
    private void trim(CorfuPayloadMsg<TrimRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        trimMap.compute(msg.getPayload().getStream(), (key, prev) ->
                prev == null ? msg.getPayload().getPrefix() : Math.max(prev, msg.getPayload().getPrefix()));
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    /**
     * Retrieve the LogUnitEntry from disk, given an address.
     *
     * @param logAddress The address to retrieve the entry from.
     * @return The log unit entry to retrieve into the cache.
     * This function should not care about trimmed addresses, as that is handled in
     * the read() and write(). Any address that cannot be retrieved should be returned as
     * unwritten (null).
     */
    public synchronized LogData handleRetrieval(LogAddress logAddress) {
        LogData entry = streamLog.read(logAddress);
        log.trace("Retrieved[{} : {}]", logAddress, entry);
        return entry;
    }


    public synchronized void handleEviction(LogAddress address, LogData entry, RemovalCause cause) {
        log.trace("Eviction[{}]: {}", address, cause);
        streamLog.release(address, entry);
    }


    public void runGC() {
        Thread.currentThread().setName("LogUnit-GC");
        val retry = IRetry.build(IntervalAndSentinelRetry.class, this::handleGC)
                .setOptions(x -> x.setSentinelReference(running))
                .setOptions(x -> x.setRetryInterval(SMALL_INTERVAL.toMillis()));

        gcRetry = (IntervalAndSentinelRetry) retry;

        retry.runForever();
    }

    @SuppressWarnings("unchecked")
    public boolean handleGC() {
        log.info("Garbage collector starting...");
        long freedEntries = 0;

        /* Pick a non-compacted region or just scan the cache */
        Map<LogAddress, LogData> map = dataCache.asMap();
        SortedSet<LogAddress> addresses = new TreeSet<>(map.keySet());
        for (LogAddress address : addresses) {
            LogData buffer = dataCache.getIfPresent(address);
            if (buffer != null) {
                Set<UUID> streams = buffer.getStreams();
                // this is a normal entry
                if (streams.size() > 0) {
                    boolean trimmable = true;
                    for (java.util.UUID stream : streams) {
                        Long trimMark = trimMap.getOrDefault(stream, null);
                        // if the stream has not been trimmed, or has not been trimmed to this point
                        if (trimMark == null || address.getAddress() > trimMark) {
                            trimmable = false;
                            break;
                        }
                        // it is not trimmable.
                    }
                    if (trimmable) {
                        log.trace("Trimming entry at {}", address);
                        trimEntry(address.getAddress(), streams, buffer);
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

    public void trimEntry(long address, Set<java.util.UUID> streams, LogData entry) {
        // Add this entry to the trimmed range map.
        //trimRange.add(Range.closed(address, address));
        // Invalidate this entry from the cache. This will cause the CacheLoader to free the entry from the disk
        // assuming the entry is back by disk
        dataCache.invalidate(address);
    }

    /**
     * Shutdown the server.
     */
    @Override
    public void shutdown() {
        scheduler.shutdownNow();
        batchWriter.close();
    }

    @VisibleForTesting
    LoadingCache<LogAddress, LogData> getDataCache() {
        return dataCache;
    }
}
