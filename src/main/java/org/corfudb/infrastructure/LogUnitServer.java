package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.corfudb.infrastructure.log.*;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.util.Utils;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalAndSentinelRetry;

import javax.annotation.Nonnull;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
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

    private ServerContext serverContext;

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

    /** Handler for the base server */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
                                            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * Service an incoming write request.
     */
    @ServerHandler(type=CorfuMsgType.WRITE)
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
                // In replex stream mode, we allocate a local token first, and use it as the
                // stream address.
                //Long token = getLog(msg.getPayload().getStreamID()).getToken(1);
                for (UUID streamID : msg.getPayload().getStreamAddresses().keySet()) {
                    dataCache.put(new LogAddress(msg.getPayload().getStreamAddresses().get(streamID), streamID),
                            msg.getPayload().getData());
                }
                r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());
            }
        } catch (Exception ex) {
            if (msg.getPayload().getWriteMode() != WriteMode.REPLEX_STREAM)
                r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE.msg());
            else
                r.sendResponse(ctx, msg, CorfuMsgType.ERROR_REPLEX_OVERWRITE.msg());
        }
    }

    /**
     * Service an incoming commit request.
     */
    @ServerHandler(type=CorfuMsgType.COMMIT)
    public void commit(CorfuPayloadMsg<CommitRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        Map<UUID, Long> streamAddresses = msg.getPayload().getStreams();
        if (streamAddresses == null) {
            // Then this is a commit bit for the global log.
            LogData entry = dataCache.get(new LogAddress(msg.getPayload().getAddress(), null));
            if (entry == null) {
                r.sendResponse(ctx, msg, CorfuMsgType.ERROR_NOENTRY.msg());
                return;
            }
            else {
                entry.getMetadataMap().put(IMetadata.LogUnitMetadataType.COMMIT, msg.getPayload().getCommit());
            }
        } else {
            for (UUID streamID : msg.getPayload().getStreams().keySet()) {
                LogData entry = dataCache.get(new LogAddress(streamAddresses.get(streamID), streamID));
                if (entry == null) {
                    r.sendResponse(ctx, msg, CorfuMsgType.ERROR_NOENTRY.msg());
                    // TODO: Crap, we have to go back and undo all the commit bits??
                    return;
                }
                else {
                    entry.getMetadataMap().put(IMetadata.LogUnitMetadataType.COMMIT, msg.getPayload().getCommit());
                }
            }
        }
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type=CorfuMsgType.STREAM_TOKEN)
    private void stream_token(CorfuPayloadMsg<UUID> msg, ChannelHandlerContext ctx, IServerRouter r) {
        r.sendResponse(ctx, msg, CorfuMsgType.STREAM_TOKEN_RESPONSE.payloadMsg(getLog(msg.getPayload()).getToken(0)));
    }

    @ServerHandler(type=CorfuMsgType.READ_REQUEST)
    private void read(CorfuPayloadMsg<ReadRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.debug("log read: {} {}", msg.getPayload().getStreamID(), msg.getPayload().getRange());
        ReadResponse rr = new ReadResponse();
        for (Long l = msg.getPayload().getRange().lowerEndpoint();
             l < msg.getPayload().getRange().upperEndpoint()+1L; l++) {
            LogData e = dataCache.get(new LogAddress(l, msg.getPayload().getStreamID()));
            if (e == null) {
                rr.put(l, LogData.EMPTY);
            } else if (e.getType() == DataType.HOLE) {
                rr.put(l, LogData.HOLE);
            } else {
                rr.put(l, e);
            }
        }
        r.sendResponse(ctx, msg, CorfuMsgType.READ_RESPONSE.payloadMsg(rr));
    }

    @ServerHandler(type=CorfuMsgType.GC_INTERVAL)
    private void gc_interval(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx, IServerRouter r) {
        gcRetry.setRetryInterval(msg.getPayload());
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type=CorfuMsgType.FORCE_GC)
    private void force_gc(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        gcThread.interrupt();
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type=CorfuMsgType.FILL_HOLE)
    private void fill_hole(CorfuPayloadMsg<TrimRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        dataCache.get(new LogAddress(msg.getPayload().getPrefix(), msg.getPayload().getStream()), x -> LogData.HOLE);
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type=CorfuMsgType.TRIM)
    private void trim(CorfuPayloadMsg<TrimRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        trimMap.compute(msg.getPayload().getStream(), (key, prev) ->
                prev == null ? msg.getPayload().getPrefix() : Math.max(prev, msg.getPayload().getPrefix()));
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }


    /**
     * The garbage collection thread.
     */
    Thread gcThread;

    ConcurrentHashMap<UUID, Long> trimMap;
    IntervalAndSentinelRetry gcRetry;
    AtomicBoolean running = new AtomicBoolean(true);
    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent storage.
     */
    LoadingCache<LogAddress, LogData> dataCache;
    long maxCacheSize;

    private AbstractLocalLog localLog;

    public static long maxLogFileSize = Integer.MAX_VALUE;  // 2GB by default

    private final ConcurrentHashMap<UUID, AbstractLocalLog> streamLogs = new ConcurrentHashMap<>();

    private AbstractLocalLog getLog(UUID stream) {
        if (stream == null) return localLog;
        else {
            return streamLogs.computeIfAbsent(stream, x-> {
                if ((Boolean) opts.get("--memory")) {
                    return new InMemoryLog(0, Long.MAX_VALUE);
                }
                else {
                    String logdir = opts.get("--log-path") + File.separator + "log" + File.separator + stream;
                    return new RollingLog(0, Long.MAX_VALUE, logdir, (Boolean) opts.get("--sync"));
                }
            });
        }
    }

    public LogUnitServer(ServerContext serverContext) {
        this.opts = serverContext.getServerConfig();
        this.serverContext = serverContext;

        maxCacheSize = Utils.parseLong(opts.get("--max-cache"));
        if (opts.get("--quickcheck-test-mode") != null &&
            (Boolean) opts.get("--quickcheck-test-mode")) {
            // It's really annoying when using OS X + HFS+ that HFS+ does not
            // support sparse files.  If we use the default 2GB file size, then
            // every time that a sparse file is closed, the OS will always
            // write 2GB of data to disk.  {sadpanda}  Use this static class
            // var to signal to RollingLog to use a smaller file size.
            maxLogFileSize = 4_000_000;
        }

        reboot();

/*       compactTail seems to be broken, disabling it for now
         scheduler.scheduleAtFixedRate(this::compactTail,
                Utils.getOption(opts, "--compact", Long.class, 60L),
                Utils.getOption(opts, "--compact", Long.class, 60L),
                TimeUnit.SECONDS);*/

        gcThread = new Thread(this::runGC);
        gcThread.start();
    }

    @Override
    public void reset() {
        String d = serverContext.getDataStore().getLogDir();
        localLog.close();
        if (d != null) {
            Path dir = FileSystems.getDefault().getPath(d);
            String prefixes[] = new String[]{"log"};

            for (String pfx : prefixes) {
                try (DirectoryStream<Path> stream =
                             Files.newDirectoryStream(dir, pfx + "*")) {
                    for (Path entry : stream) {
                        // System.out.println("Deleting " + entry);
                        Files.delete(entry);
                    }
                } catch (IOException e) {
                    log.error("reset: error deleting prefix " + pfx + ": " + e.toString());
                }
            }
        }
        reboot();
    }

    @Override
    public void reboot() {
        if ((Boolean) opts.get("--memory")) {
            log.warn("Log unit opened in-memory mode (Maximum size={}). " +
                    "This should be run for testing purposes only. " +
                    "If you exceed the maximum size of the unit, old entries will be AUTOMATICALLY trimmed. " +
                    "The unit WILL LOSE ALL DATA if it exits.", Utils.convertToByteStringRepresentation(maxCacheSize));
            localLog = new InMemoryLog(0, Long.MAX_VALUE);
        } else {
            String logdir = opts.get("--log-path") + File.separator + "log";
            localLog = new RollingLog(0, Long.MAX_VALUE, logdir, (Boolean) opts.get("--sync"));
        }

        if (dataCache != null) {
            /** Free all references */
            dataCache.asMap().values().parallelStream()
                    .map(m -> m.getData().release());
        }

        dataCache = Caffeine.<LogAddress,LogData>newBuilder()
                .<LogAddress,LogData>weigher((k, v) -> v.getData() == null ? 1 : v.getData().readableBytes())
                .maximumWeight(maxCacheSize)
                .removalListener(this::handleEviction)
                .writer(new CacheWriter<LogAddress, LogData>() {
                    @Override
                    public void write(@Nonnull LogAddress address, @Nonnull LogData entry) {
                        if (dataCache.getIfPresent(address) != null) {
                            throw new RuntimeException("overwrite");
                        }
                        //TODO - persisted entries should be of type PersistedLogData
                        //if (!entry.isPersisted) { //don't persist an entry twice.
                        if (address.getStream() != null) {
                            getLog(address.getStream()).write(address.getAddress(), entry);
                        }
                        else {
                            localLog.write(address.getAddress(), entry);
                        }
                        //    }
                    }

                    @Override
                    public void delete(LogAddress aLong, LogData logUnitEntry, RemovalCause removalCause) {
                        // never need to delete
                    }
                }).<LogAddress,LogData>build(this::handleRetrieval);

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
    public synchronized LogData handleRetrieval(LogAddress address) {
        LogData entry;
        if (address.getStream() != null) {
            entry = getLog(address.getStream()).read(address.getAddress());
        }
        else {
            entry = localLog.read(address.getAddress());
        }
        log.trace("Retrieved[{} : {}]", address, entry);
        return entry;
    }

    public synchronized void handleEviction(LogAddress address, LogData entry, RemovalCause cause) {
        log.trace("Eviction[{}]: {}", address, cause);
        if (entry.getData() != null) {
            // Free the internal buffer once the data has been evicted (in the case the server is not sync).
            entry.getData().release();
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
        //and free any references the buffer might have
        if (entry.getData() != null) {
            entry.getData().release();
        }
    }

    /**
     * Shutdown the server.
     */
    @Override
    public void shutdown() {
        scheduler.shutdownNow();
        dataCache.invalidateAll(); //should evict all entries
    }

    @VisibleForTesting
    LoadingCache<LogAddress, LogData> getDataCache() {
        return dataCache;
    }
}
