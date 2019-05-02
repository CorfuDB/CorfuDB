package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelHandlerContext;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.InMemoryStreamLog;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.StreamLogCompaction;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.FillHoleRequest;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.MultipleReadRequest;
import org.corfudb.protocols.wireprotocol.RangeWriteMsg;
import org.corfudb.protocols.wireprotocol.ReadRequest;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsRequest;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TrimRequest;
import org.corfudb.protocols.wireprotocol.WriteRequest;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.LogUnitException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.Utils;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.corfudb.infrastructure.BatchWriterOperation.Type.*;


/**
 * Created by mwei on 12/10/15.
 *
 * <p>A Log Unit Server, which is responsible for providing the persistent storage for the Corfu
 * Distributed Shared Log.
 *
 * <p>All reads and writes go through a cache. For persistence, every 10,000 log entries are written
 * to individual files (logs), which are represented as FileHandles. Each FileHandle contains a
 * pointer to the tail of the file, a memory-mapped file channel, and a set of addresses known
 * to be in the file. To append an entry, the pointer to the tail is first extended to the
 * length of the entry, and the entry is added to the set of known addresses. A header is
 * written, which consists of the ASCII characters LE, followed by a set of flags, the log unit
 * address, the size of the entry, then the metadata size, metadata and finally the entry itself
 * . When the entry is complete, a written flag is set in the flags field.
 */
@Slf4j
public class LogUnitServer extends AbstractServer {

    /**
     * The options map.
     */
    private final LogUnitServerConfig config;

    /**
     * The server context of the node.
     */
    private final ServerContext serverContext;

    /**
     * Handler for this server.
     */
    @Getter
    private final CorfuMsgHandler handler = CorfuMsgHandler.generateHandler(MethodHandles.lookup(), this);

    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent
     * storage.
     */
    private final LoadingCache<Long, ILogData> dataCache;
    private final StreamLog streamLog;
    private final StreamLogCompaction logCleaner;
    private final BatchProcessor batchWriter;

    private ExecutorService executor;

    @Override
    public ExecutorService getExecutor(CorfuMsgType corfuMsgType) {
        return executor;
    }

    @Override
    public List<ExecutorService> getExecutors() {
        return Collections.singletonList(executor);
    }

    /**
     * Returns a new LogUnitServer.
     *
     * @param serverContext context object providing settings and objects
     */
    public LogUnitServer(ServerContext serverContext) {
        this.serverContext = serverContext;
        this.config = LogUnitServerConfig.parse(serverContext.getServerConfig());
        executor = Executors.newFixedThreadPool(serverContext.getLogunitThreadCount(),
                new ServerThreadFactory("LogUnit-", new ServerThreadFactory.ExceptionHandler()));

        if (config.isMemoryMode()) {
            log.warn("Log unit opened in-memory mode (Maximum size={}). "
                    + "This should be run for testing purposes only. "
                    + "If you exceed the maximum size of the unit, old entries will be "
                    + "AUTOMATICALLY trimmed. "
                    + "The unit WILL LOSE ALL DATA if it exits.", Utils
                    .convertToByteStringRepresentation(config.getMaxCacheSize()));
            streamLog = new InMemoryStreamLog();
        } else {
            streamLog = new StreamLogFiles(serverContext, config.isNoVerify());
        }

        batchWriter = new BatchProcessor(streamLog, serverContext.getServerEpoch(), !config.isNoSync());

        dataCache = Caffeine.newBuilder()
                .<Long, ILogData>weigher((k, v) -> ((LogData) v).getData() == null ? 1 : ((LogData) v).getData().length)
                .maximumWeight(config.getMaxCacheSize())
                .removalListener(this::handleEviction)
                .build(this::handleRetrieval);

        logCleaner = new StreamLogCompaction(streamLog, 10, 45, TimeUnit.MINUTES, ServerContext.SHUTDOWN_TIMER);
    }

    /**
     * Service an incoming request for maximum global address the log unit server has written.
     */
    @ServerHandler(type = CorfuMsgType.TAIL_REQUEST)
    public void handleTailRequest(CorfuPayloadMsg<TailsRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.debug("handleTailRequest: received a tail request {}", msg);
        batchWriter.<TailsResponse>addTask(TAILS_QUERY, msg)
                .thenAccept(tailsResp -> r.sendResponse(ctx, msg, CorfuMsgType.TAIL_RESPONSE.payloadMsg(tailsResp)))
                .exceptionally(ex -> {
                    handleException(ex, ctx, msg, r);
                    return null;
                });
    }

    /**
     * Service an incoming request for log address space, i.e., the map of addresses for every stream in the log.
     * This is used on sequencer bootstrap to provide the address maps for initialization.
     */
    @ServerHandler(type = CorfuMsgType.LOG_ADDRESS_SPACE_REQUEST)
    public void handleLogAddressSpaceRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        CorfuPayloadMsg<Void> payloadMsg = new CorfuPayloadMsg<>();
        payloadMsg.copyBaseFields(msg);
        log.debug("handleLogAddressSpaceRequest: received a log address space request {}", msg);
        batchWriter.<StreamsAddressResponse>addTask(LOG_ADDRESS_SPACE_QUERY, payloadMsg)
                .thenAccept(tailsResp -> r.sendResponse(ctx, msg,
                        CorfuMsgType.LOG_ADDRESS_SPACE_RESPONSE.payloadMsg(tailsResp)))
                .exceptionally(ex -> {
                    handleException(ex, ctx, payloadMsg, r);
                    return null;
                });
    }

    /**
     * Service an incoming request to retrieve the starting address of this logging unit.
     */
    @ServerHandler(type = CorfuMsgType.TRIM_MARK_REQUEST)
    public void handleTrimMarkRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.debug("handleTrimMarkRequest: received a trim mark request {}", msg);
        r.sendResponse(ctx, msg, CorfuMsgType.TRIM_MARK_RESPONSE.payloadMsg(streamLog.getTrimMark()));
    }

    /**
     * A helper function that maps an exception to the appropriate response message.
     */
    void handleException(Throwable ex, ChannelHandlerContext ctx, CorfuPayloadMsg msg, IServerRouter r) {
        log.trace("handleException: handling exception {} for {}", ex, msg);
        if (ex.getCause() instanceof WrongEpochException) {
            WrongEpochException wee = (WrongEpochException) ex.getCause();
            r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, wee.getCorrectEpoch()));
        } else if (ex.getCause() instanceof OverwriteException) {
            OverwriteException owe = (OverwriteException) ex.getCause();
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE
                    .payloadMsg(owe.getOverWriteCause().getId()));
        } else if (ex.getCause() instanceof DataOutrankedException) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_OUTRANKED.msg());
        } else if (ex.getCause() instanceof ValueAdoptedException) {
            ValueAdoptedException vae = (ValueAdoptedException) ex.getCause();
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_VALUE_ADOPTED.payloadMsg(vae.getReadResponse()));
        } else if (ex.getCause() instanceof TrimmedException) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_TRIMMED.msg());
        } else {
            throw new LogUnitException(ex);
        }
    }

    /**
     * Service an incoming write request.
     */
    @ServerHandler(type = CorfuMsgType.WRITE)
    public void write(CorfuPayloadMsg<WriteRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.debug("log write: global: {}, streams: {}", msg.getPayload().getToken(),
                msg.getPayload().getData().getBackpointerMap());
        LogData logData = (LogData) msg.getPayload().getData();

        batchWriter.addTask(WRITE, msg)
                .thenRunAsync(() -> {
                    dataCache.put(msg.getPayload().getGlobalAddress(), logData);
                    r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());
                }, executor).exceptionally(ex -> {
                    handleException(ex, ctx, msg, r);
                    return null;
        });
    }

    /**
     * Services incoming range write calls.
     */
    @ServerHandler(type = CorfuMsgType.RANGE_WRITE)
    public void rangeWrite(CorfuPayloadMsg<RangeWriteMsg> msg,
                           ChannelHandlerContext ctx, IServerRouter r) {
        List<LogData> range = msg.getPayload().getEntries();
        log.debug("rangeWrite: Writing {} entries [{}-{}]", range.size(),
                range.get(0).getGlobalAddress(), range.get(range.size() - 1).getGlobalAddress());

        batchWriter.addTask(RANGE_WRITE, msg)
                .thenRun(() -> r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg()))
                .exceptionally(ex -> {
                    handleException(ex, ctx, msg, r);
                    return null;
                });
    }

    @ServerHandler(type = CorfuMsgType.FILL_HOLE)
    private void fillHole(CorfuPayloadMsg<FillHoleRequest> msg, ChannelHandlerContext ctx,
                          IServerRouter r) {
        Token address = msg.getPayload().getAddress();
        log.debug("fillHole: filling address at {}, epoch {}", address, msg.getEpoch());

        // A hole fill is converted to a regular write in order to reuse the same
        // write-path.
        LogData hole = LogData.getHole(address.getSequence());
        hole.setEpoch(address.getEpoch());
        CorfuPayloadMsg<WriteRequest> writeReq = new CorfuPayloadMsg<>(CorfuMsgType.WRITE, new WriteRequest(hole));
        writeReq.copyBaseFields(msg);

        batchWriter.addTask(WRITE, writeReq)
                .thenRunAsync(() -> {
                    dataCache.put(address.getSequence(), writeReq.getPayload().getData());
                    r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());
                }, executor).exceptionally(ex -> {
                    handleException(ex, ctx, msg, r);
                    return null;
                });
    }

    /**
     * Perform a prefix trim.
     * Here the token is not used to perform the trim as the epoch at which the checkpoint was completed
     * might be old. Hence, we use the msg epoch to perform the trim. This should be safe provided that the
     * trim is performed only on the token provided by the CheckpointWriter which ensures that the checkpoint
     * was persisted. Using any other address to perform a trim can cause data loss.
     */
    @ServerHandler(type = CorfuMsgType.PREFIX_TRIM)
    private void prefixTrim(CorfuPayloadMsg<TrimRequest> msg, ChannelHandlerContext ctx,
                            IServerRouter r) {
        log.debug("prefixTrim: trimming prefix to {}", msg.getPayload().getAddress());
        batchWriter.addTask(PREFIX_TRIM, msg)
                .thenRun(() -> r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg()))
                .exceptionally(ex -> {
                    handleException(ex, ctx, msg, r);
                    return null;
                });
    }

    @ServerHandler(type = CorfuMsgType.READ_REQUEST)
    private void read(CorfuPayloadMsg<ReadRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("read: {}", msg.getPayload().getRange());
        ReadResponse rr = new ReadResponse();
        try {
            for (Long l = msg.getPayload().getRange().lowerEndpoint();
                 l < msg.getPayload().getRange().upperEndpoint() + 1L; l++) {
                ILogData e = dataCache.get(l);
                if (e == null) {
                    rr.put(l, LogData.getEmpty(l));
                } else {
                    rr.put(l, (LogData) e);
                }
            }
            r.sendResponse(ctx, msg, CorfuMsgType.READ_RESPONSE.payloadMsg(rr));
        } catch (DataCorruptionException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_CORRUPTION.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.MULTIPLE_READ_REQUEST)
    private void multiRead(CorfuPayloadMsg<MultipleReadRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("multiRead: {}", msg.getPayload().getAddresses());

        ReadResponse rr = new ReadResponse();
        try {
            for (Long l : msg.getPayload().getAddresses()) {
                ILogData e = dataCache.get(l);
                if (e == null) {
                    rr.put(l, LogData.getEmpty(l));
                } else {
                    rr.put(l, (LogData) e);
                }
            }
            r.sendResponse(ctx, msg, CorfuMsgType.READ_RESPONSE.payloadMsg(rr));
        } catch (DataCorruptionException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_CORRUPTION.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.COMPACT_REQUEST)
    private void handleCompactRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.debug("handleCompactRequest: received a compact request {}", msg);
        streamLog.compact();
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type = CorfuMsgType.FLUSH_CACHE)
    private void handleFlushCacheRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.debug("handleFlushCacheRequest: received a cache flush request {}", msg);
        dataCache.invalidateAll();
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    /**
     * Seal the server with the epoch.
     * <p>
     * - A seal operation is inserted in the queue and then we wait to flush all operations
     * in the queue before this operation.
     * - All operations after this operation but stamped with an older epoch will be failed.
     */
    @Override
    public void sealServerWithEpoch(long epoch) {
        CorfuPayloadMsg<Long> msg = new CorfuPayloadMsg<>();
        msg.setEpoch(epoch);
        try {
            batchWriter.addTask(SEAL, msg).join();
        } catch (CompletionException ce) {
            if (ce.getCause() instanceof WrongEpochException) {
                // The BaseServer expects to observe this exception,
                // when it happens, so it needs to be unwrapped
                throw (WrongEpochException) ce.getCause();
            }
        }
        log.info("LogUnit sealServerWithEpoch: sealed and flushed with epoch {}", epoch);
    }

    @Override
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        return getState() == ServerState.READY;
    }

    /**
     * Resets the log unit server via the BatchProcessor.
     * Warning: Clears all data.
     * <p>
     * - The epochWaterMark is set to prevent resetting log unit multiple times during
     * same epoch.
     * - After this the reset operation is inserted which resets and clears all data.
     * - Finally the cache is invalidated to purge the existing entries.
     */
    @ServerHandler(type = CorfuMsgType.RESET_LOGUNIT)
    private synchronized void resetLogUnit(CorfuPayloadMsg<Long> msg,
                                           ChannelHandlerContext ctx, IServerRouter r) {

        // Check if the reset request is with an epoch greater than the last reset epoch seen to
        // prevent multiple reset in the same epoch. and should be equal to the current router
        // epoch to prevent stale reset requests from wiping out the data.
        if (msg.getPayload() > serverContext.getLogUnitEpochWaterMark()
                && msg.getPayload() == serverContext.getServerEpoch()) {
            serverContext.setLogUnitEpochWaterMark(msg.getPayload());
            batchWriter.addTask(RESET, msg)
                    .thenRun(() -> {
                        dataCache.invalidateAll();
                        log.info("LogUnit Server Reset.");
                        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
                    }).exceptionally(ex -> {
                        handleException(ex, ctx, msg, r);
                        return null;
                    });
        } else {
            log.info("LogUnit Server Reset request received but reset already done.");
            r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        }
    }

    /**
     * Retrieve the LogUnitEntry from disk, given an address.
     *
     * @param address The address to retrieve the entry from.
     * @return The log unit entry to retrieve into the cache.
     * <p>
     * This function should not care about trimmed addresses, as that is handled in
     * the read() and append(). Any address that cannot be retrieved should be returned as
     * unwritten (null).
     */
    private synchronized ILogData handleRetrieval(long address) {
        LogData entry = streamLog.read(address);
        log.trace("Retrieved[{} : {}]", address, entry);
        return entry;
    }

    private synchronized void handleEviction(long address, ILogData entry, RemovalCause cause) {
        log.trace("Eviction[{}]: {}", address, cause);
    }

    /**
     * Shutdown the server.
     */
    @Override
    public void shutdown() {
        super.shutdown();
        logCleaner.shutdown();
        batchWriter.close();
    }

    @VisibleForTesting
    public LoadingCache<Long, ILogData> getDataCache() {
        return dataCache;
    }

    @VisibleForTesting
    long getMaxCacheSize() {
        return config.getMaxCacheSize();
    }

    @VisibleForTesting
    BatchProcessor getBatchWriter() {
        return batchWriter;
    }

    // The following methods should only be used for unit tests, ideally the executor should be
    // final, but this "hack" is needed to now when a task as completed
    @VisibleForTesting
    void startHandler() {
        executor = Executors.newFixedThreadPool(serverContext.getLogunitThreadCount(),
                new ServerThreadFactory("LogUnit-", new ServerThreadFactory.ExceptionHandler()));
    }

    @VisibleForTesting
    void stopHandler() throws Exception {
        executor.shutdown();
        executor.awaitTermination(ServerContext.SHUTDOWN_TIMER.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Log unit server configuration class
     */
    @Builder
    @Getter
    public static class LogUnitServerConfig {
        private final double cacheSizeHeapRatio;
        private final long maxCacheSize;
        private final boolean memoryMode;
        private final boolean noVerify;
        private final boolean noSync;

        /**
         * Parse legacy configuration options
         *
         * @param opts legacy config
         * @return log unit configuration
         */
        public static LogUnitServerConfig parse(Map<String, Object> opts) {
            double cacheSizeHeapRatio = Double.parseDouble((String) opts.get("--cache-heap-ratio"));

            return LogUnitServerConfig.builder()
                    .cacheSizeHeapRatio(cacheSizeHeapRatio)
                    .maxCacheSize((long) (Runtime.getRuntime().maxMemory() * cacheSizeHeapRatio))
                    .memoryMode(Boolean.valueOf(opts.get("--memory").toString()))
                    .noVerify((Boolean) opts.get("--no-verify"))
                    .noSync((Boolean) opts.get("--no-sync"))
                    .build();
        }
    }
}
