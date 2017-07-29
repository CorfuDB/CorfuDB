package org.corfudb.infrastructure;

import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.log.InMemoryStreamLog;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.MultipleReadRequest;
import org.corfudb.protocols.wireprotocol.ReadRequest;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.TrimRequest;
import org.corfudb.protocols.wireprotocol.WriteRequest;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.Utils;


/**
 * Created by mwei on 12/10/15.
 *
 * <p>A Log Unit Server, which is responsible for providing the persistent storage for the Corfu
 * Distributed Shared Log.
 *
 * <p>All reads and writes go through a cache. For persistence, every 10,000 log entries are written
 * to individual
 * files (logs), which are represented as FileHandles. Each FileHandle contains a pointer to the
 * tail of the file, a
 * memory-mapped file channel, and a set of addresses known to be in the file. To append an
 * entry, the pointer to the
 * tail is first extended to the length of the entry, and the entry is added to the set of known
 * addresses. A header
 * is written, which consists of the ASCII characters LE, followed by a set of flags, the log
 * unit address, the size
 * of the entry, then the metadata size, metadata and finally the entry itself. When the entry is
 * complete, a written
 * flag is set in the flags field.
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

    private ScheduledFuture<?> compactor;

    /**
     * The options map.
     */
    private final Map<String, Object> opts;

    /**
     * Handler for the base server.
     */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent
     * storage.
     */
    private final LoadingCache<Long, ILogData> dataCache;
    private final long maxCacheSize;

    private final StreamLog streamLog;

    private final BatchWriter<Long, ILogData> batchWriter;

    private static final String metricsPrefix = "corfu.server.logunit.";

    /**
     * Returns a new LogUnitServer.
     * @param serverContext context object providing settings and objects
     */
    public LogUnitServer(ServerContext serverContext) {
        this.opts = serverContext.getServerConfig();
        double cacheSizeHeapRatio = Double.parseDouble((String) opts.get("--cache-heap-ratio"));

        maxCacheSize = (long) (Runtime.getRuntime().maxMemory() * cacheSizeHeapRatio);

        if ((Boolean) opts.get("--memory")) {
            log.warn("Log unit opened in-memory mode (Maximum size={}). "
                    + "This should be run for testing purposes only. "
                    + "If you exceed the maximum size of the unit, old entries will be "
                    + "AUTOMATICALLY trimmed. "
                    + "The unit WILL LOSE ALL DATA if it exits.", Utils
                    .convertToByteStringRepresentation(maxCacheSize));
            streamLog = new InMemoryStreamLog();
        } else {
            streamLog = new StreamLogFiles(serverContext, (Boolean) opts.get("--no-verify"));
        }

        batchWriter = new BatchWriter(streamLog);

        dataCache = Caffeine.<Long, ILogData>newBuilder()
                .<Long, ILogData>weigher((k, v) -> ((LogData) v).getData() == null ? 1 : (
                        (LogData) v).getData().length)
                .maximumWeight(maxCacheSize)
                .removalListener(this::handleEviction)
                .writer(batchWriter)
                .recordStats()
                .build(this::handleRetrieval);

        MetricRegistry metrics = serverContext.getMetrics();
        MetricsUtils.addCacheGauges(metrics, metricsPrefix + "cache.", dataCache);

        Runnable task = () -> streamLog.compact();
        compactor = scheduler.scheduleAtFixedRate(task, 10, 45, TimeUnit.MINUTES);
    }

    /**
     * Service an incoming request for maximum global address the log unit server has written.
     */
    @ServerHandler(type = CorfuMsgType.TAIL_REQUEST, opTimer = metricsPrefix + "tailReq")
    public void handleTailRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r,
                                  boolean isMetricsEnabled) {
        r.sendResponse(ctx, msg, CorfuMsgType.TAIL_RESPONSE.payloadMsg(streamLog.getGlobalTail()));
    }

    /**
     * Service an incoming request to retrieve the starting address of this logging unit.
     */
    @ServerHandler(type = CorfuMsgType.TRIM_MARK_REQUEST, opTimer = metricsPrefix + "headReq")
    public void handleHeadRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r, boolean isMetricsEnabled) {
        r.sendResponse(ctx, msg, CorfuMsgType.TRIM_MARK_RESPONSE.payloadMsg(streamLog.getTrimMark()));
    }

    /**
     * Service an incoming write request.
     */
    @ServerHandler(type = CorfuMsgType.WRITE, opTimer = metricsPrefix + "write")
    public void write(CorfuPayloadMsg<WriteRequest> msg, ChannelHandlerContext ctx, IServerRouter r,
                      boolean isMetricsEnabled) {
        log.debug("log write: global: {}, streams: {}, backpointers: {}", msg
                .getPayload().getGlobalAddress(), msg.getPayload().getData().getBackpointerMap());

        try {
            dataCache.put(msg.getPayload().getGlobalAddress(), msg.getPayload().getData());
            r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());

        } catch (OverwriteException ex) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE.msg());
        } catch (DataOutrankedException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_OUTRANKED.msg());
        } catch (ValueAdoptedException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_VALUE_ADOPTED.payloadMsg(e
                    .getReadResponse()));
        }
    }

    @ServerHandler(type = CorfuMsgType.READ_REQUEST, opTimer = metricsPrefix + "read")
    private void read(CorfuPayloadMsg<ReadRequest> msg, ChannelHandlerContext ctx, IServerRouter r,
                      boolean isMetricsEnabled) {
        log.trace("read: {}", msg.getPayload().getRange());
        ReadResponse rr = new ReadResponse();
        try {
            for (Long l = msg.getPayload().getRange().lowerEndpoint();
                    l < msg.getPayload().getRange().upperEndpoint() + 1L; l++) {
                ILogData e = dataCache.get(l);
                if (e == null) {
                    rr.put(l, LogData.EMPTY);
                } else {
                    rr.put(l, (LogData) e);
                }
            }
            r.sendResponse(ctx, msg, CorfuMsgType.READ_RESPONSE.payloadMsg(rr));
        } catch (DataCorruptionException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_CORRUPTION.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.MULTIPLE_READ_REQUEST, opTimer = metricsPrefix + "multiRead")
    private void multiRead(CorfuPayloadMsg<MultipleReadRequest> msg, ChannelHandlerContext ctx, IServerRouter r,
                           boolean isMetricsEnabled) {
        log.trace("multiRead: {}", msg.getPayload().getAddresses());

        ReadResponse rr = new ReadResponse();
        try {
            for (Long l : msg.getPayload().getAddresses()) {
                ILogData e = dataCache.get(l);
                if (e == null) {
                    rr.put(l, LogData.EMPTY);
                } else {
                    rr.put(l, (LogData) e);
                }
            }
            r.sendResponse(ctx, msg, CorfuMsgType.READ_RESPONSE.payloadMsg(rr));
        } catch (DataCorruptionException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_CORRUPTION.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.FILL_HOLE, opTimer = metricsPrefix + "fill-hole")
    private void fillHole(CorfuPayloadMsg<TrimRequest> msg, ChannelHandlerContext ctx,
                          IServerRouter r,
                          boolean isMetricsEnabled) {
        try {
            dataCache.put(msg.getPayload().getAddress(), LogData.HOLE);
            r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());

        } catch (OverwriteException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE.msg());
        } catch (DataOutrankedException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_OUTRANKED.msg());
        } catch (ValueAdoptedException e) {

            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_VALUE_ADOPTED.payloadMsg(e
                    .getReadResponse()));
        }
    }

    @ServerHandler(type = CorfuMsgType.TRIM, opTimer = metricsPrefix + "fill-hole")
    private void trim(CorfuPayloadMsg<TrimRequest> msg, ChannelHandlerContext ctx, IServerRouter r,
                      boolean isMetricsEnabled) {
        batchWriter.trim(msg.getPayload().getAddress());
        //TODO(Maithem): should we return an error if the write fails
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type = CorfuMsgType.PREFIX_TRIM)
    private void prefixTrim(CorfuPayloadMsg<TrimRequest> msg, ChannelHandlerContext ctx,
                            IServerRouter r,
                            boolean isMetricsEnabled) {
        try {
            batchWriter.prefixTrim(msg.getPayload().getAddress());
            r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        } catch (TrimmedException ex) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_TRIMMED.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.COMPACT_REQUEST, opTimer = metricsPrefix + "compact")
    private void compact(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r, boolean
            isMetricsEnabled) {
        try {
            streamLog.compact();
            r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        } catch (RuntimeException ex) {
            log.error("Internal Error");
            //TODO(Maithem) Need an internal error return type
            r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.FLUSH_CACHE, opTimer = metricsPrefix + "flush-cache")
    private void flushCache(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r, boolean
            isMetricsEnabled) {
        try {
            dataCache.invalidateAll();
        } catch (RuntimeException e) {
            log.error("Encountered error while flushing cache {}", e);
        }
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }


    /**
     * Retrieve the LogUnitEntry from disk, given an address.
     *
     * @param address The address to retrieve the entry from.
     * @return The log unit entry to retrieve into the cache.
     *
     *     This function should not care about trimmed addresses, as that is handled in
     *     the read() and append(). Any address that cannot be retrieved should be returned as
     *     unwritten (null).
     */
    public synchronized ILogData handleRetrieval(long address) {
        LogData entry = streamLog.read(address);
        log.trace("Retrieved[{} : {}]", address, entry);
        return entry;
    }


    public synchronized void handleEviction(long address, ILogData entry, RemovalCause cause) {
        log.trace("Eviction[{}]: {}", address, cause);
        streamLog.release(address, (LogData) entry);
    }

    /**
     * Shutdown the server.
     */
    @Override
    public void shutdown() {
        compactor.cancel(true);
        scheduler.shutdownNow();
        batchWriter.close();
    }

    @VisibleForTesting
    public LoadingCache<Long, ILogData> getDataCache() {
        return dataCache;
    }

    @VisibleForTesting
    long getMaxCacheSize() {
        return maxCacheSize;
    }
}
