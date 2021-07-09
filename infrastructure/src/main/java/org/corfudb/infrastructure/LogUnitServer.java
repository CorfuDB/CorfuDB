package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;
import io.netty.channel.ChannelHandlerContext;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.configuration.ServerConfiguration;
import org.corfudb.infrastructure.log.InMemoryStreamLog;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.StreamLogCompaction;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.CorfuProtocolLogData;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.LogUnitException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.Utils;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.corfudb.protocols.CorfuProtocolLogData.getLogData;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getDataCorruptionErrorMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getOverwriteErrorMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getTrimmedErrorMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getUnknownErrorMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getWrongEpochErrorMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getSealRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getCommittedTailResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getCompactResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getFlushCacheResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getInspectAddressesResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getKnownAddressResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getLogAddressSpaceResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getRangeWriteLogResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getReadLogResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getResetLogUnitResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTailResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTrimLogResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTrimMarkResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getUpdateCommittedTailResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getWriteLogResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getDefaultProtocolVersionMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;


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
     * RequestHandlerMethods for the LogUnit server.
     */
    @Getter
    private final RequestHandlerMethods handlerMethods =
            RequestHandlerMethods.generateHandler(MethodHandles.lookup(), this);

    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent
     * storage.
     */
    private final LogUnitServerCache dataCache;

    @Getter
    private final StreamLog streamLog;
    private final StreamLogCompaction logCleaner;
    private final BatchProcessor batchWriter;
    private final ExecutorService executor;

    /**
     * Returns a new LogUnitServer.
     *
     * @param serverContext context object providing settings and objects
     */
    public LogUnitServer(ServerContext serverContext) {
        this(serverContext, new LogUnitServerInitializer());
    }

    /**
     * Returns a new LogUnitServer.
     *
     * @param serverContext      context object providing settings and objects
     * @param serverInitializer  a LogUnitServerInitializer object used for initializing the
     *                           cache, stream log, and batch processor for this server
     */
    public LogUnitServer(ServerContext serverContext, LogUnitServerInitializer serverInitializer) {
        this.serverContext = serverContext;
        config = LogUnitServerConfig.parse(serverContext.getConfiguration());
        executor = serverContext.getExecutorService(serverContext.getConfiguration().getNumLogUnitWorkerThreads(),
                "LogUnit-");

        if (serverContext.getConfiguration().isInMemoryMode()) {
            log.warn("Log unit opened in-memory mode (Maximum size={}). "
                    + "This should be run for testing purposes only. "
                    + "If you exceed the maximum size of the unit, old entries will be "
                    + "AUTOMATICALLY trimmed. "
                    + "The unit WILL LOSE ALL DATA if it exits.", Utils
                    .convertToByteStringRepresentation(serverContext.getConfiguration().getMaxLogUnitCacheSize()));
            streamLog = serverInitializer.buildInMemoryStreamLog();
        } else {
            streamLog = serverInitializer.buildStreamLog(serverContext);
        }

        dataCache = serverInitializer.buildLogUnitServerCache(streamLog, serverContext);
        batchWriter = serverInitializer.buildBatchProcessor(streamLog, serverContext);
        logCleaner = serverInitializer.buildStreamLogCompaction(streamLog);
    }

    @Override
    protected void processRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        executor.submit(() -> getHandlerMethods().handle(req, ctx, router));
    }

    /**
     * Service an incoming request for maximum global address the log unit server has written.
     */
    @RequestHandler(type = PayloadCase.TAIL_REQUEST)
    private void handleTailRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        if (log.isTraceEnabled()) {
            log.trace("handleTailRequest[{}]: received a tail request {}",
                    req.getHeader().getRequestId(), TextFormat.shortDebugString(req));
        }

        batchWriter.<TailsResponse>addTask(BatchWriterOperation.Type.TAILS_QUERY, req)
                .thenAccept(tailsResp ->
                    // Note: we reuse the request header as the ignore_cluster_id and
                    // ignore_epoch fields are the same in both cases.
                    router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()), getTailResponseMsg(
                            tailsResp.getEpoch(), tailsResp.getLogTail(), tailsResp.getStreamTails())), ctx))
                .exceptionally(ex -> {
                    handleException(ex, ctx, req, router);
                    return null;
                });
    }

    /**
     * Service an incoming request for log address space, i.e., the map of addresses for every stream in the log.
     * This is used on sequencer bootstrap to provide the address maps for initialization.
     */
    @RequestHandler(type = PayloadCase.LOG_ADDRESS_SPACE_REQUEST)
    private void handleLogAddressSpaceRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        if (log.isTraceEnabled()) {
            log.trace("handleLogAddressSpaceRequest[{}]: received a log " +
                    "address space request {}", req.getHeader().getRequestId(), TextFormat.shortDebugString(req));
        }

        batchWriter.<StreamsAddressResponse>addTask(BatchWriterOperation.Type.LOG_ADDRESS_SPACE_QUERY, req)
                .thenAccept(resp ->
                    // Note: we reuse the request header as the ignore_cluster_id and
                    // ignore_epoch fields are the same in both cases.
                    router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()),
                            getLogAddressSpaceResponseMsg(resp.getLogTail(), resp.getEpoch(), resp.getAddressMap())), ctx))
                .exceptionally(ex -> {
                    handleException(ex, ctx, req, router);
                    return null;
                });
    }

    /**
     * Service an incoming request to retrieve the starting address of this logging unit.
     */
    @RequestHandler(type = PayloadCase.TRIM_MARK_REQUEST)
    private void handleTrimMarkRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        if (log.isTraceEnabled()) {
            log.trace("handleTrimMarkRequest: received a trim mark request {}", TextFormat.shortDebugString(req));
        }

        // Note: we reuse the request header as the ignore_cluster_id and
        // ignore_epoch fields are the same in both cases.
        router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()),
                getTrimMarkResponseMsg(streamLog.getTrimMark())), ctx);
    }

    /**
     * Service an incoming query for the committed tail on this log unit server.
     */
    @RequestHandler(type = PayloadCase.COMMITTED_TAIL_REQUEST)
    private void handleCommittedTailRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        if (log.isTraceEnabled()) {
            log.trace("handleCommittedTailRequest: received a "
                    + "committed log tail request {}", TextFormat.shortDebugString(req));
        }

        // Note: we reuse the request header as the ignore_cluster_id and
        // ignore_epoch fields are the same in both cases.
        router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()),
                getCommittedTailResponseMsg(streamLog.getCommittedTail())), ctx);
    }

    /**
     * Service an incoming request to update the current committed tail.
     */
    @RequestHandler(type = PayloadCase.UPDATE_COMMITTED_TAIL_REQUEST)
    private void handleUpdateCommittedTailRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        if (log.isTraceEnabled()) {
            log.trace("handleUpdateCommittedTailRequest: received request to "
                    + "update committed tail {}", TextFormat.shortDebugString(req));
        }

        streamLog.updateCommittedTail(req.getPayload().getUpdateCommittedTailRequest().getCommittedTail());
        HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        router.sendResponse(getResponseMsg(responseHeader, getUpdateCommittedTailResponseMsg()), ctx);
    }

    /**
     * A helper function that maps an exception to the appropriate response message.
     */
    private void handleException(Throwable ex, ChannelHandlerContext ctx, RequestMsg req, IServerRouter router) {
        if (log.isTraceEnabled()) {
            log.trace("handleException: handling exception {} for {}", ex, TextFormat.shortDebugString(req));
        }

        HeaderMsg responseHeader;

        if (ex.getCause() instanceof WrongEpochException) {
            WrongEpochException wee = (WrongEpochException) ex.getCause();
            responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            router.sendResponse(getResponseMsg(responseHeader, getWrongEpochErrorMsg(wee.getCorrectEpoch())), ctx);
        } else if (ex.getCause() instanceof OverwriteException) {
            OverwriteException owe = (OverwriteException) ex.getCause();
            responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            router.sendResponse(getResponseMsg(responseHeader, getOverwriteErrorMsg(owe.getOverWriteCause().getId())), ctx);
        } else if (ex.getCause() instanceof TrimmedException) {
            responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.CHECK);
            router.sendResponse(getResponseMsg(responseHeader, getTrimmedErrorMsg()), ctx);
        } else {
            responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            router.sendResponse(getResponseMsg(responseHeader, getUnknownErrorMsg(ex)), ctx);
            throw new LogUnitException(ex);
        }
    }

    /**
     * Service an incoming write request.
     */
    @RequestHandler(type = PayloadCase.WRITE_LOG_REQUEST)
    private void handleWrite(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        LogData logData = getLogData(req.getPayload().getWriteLogRequest().getLogData());

        log.debug("handleWrite: type: {}, address: {}, streams: {}",
                logData.getType(), logData.getToken(), logData.getBackpointerMap());

        // Its not clear that making all holes high priority is the right thing to do, but since
        // some reads will block until a hole is filled this is required (i.e. bypass quota checks)
        // because the requirement is to allow reads, but only block writes once the quota is exhausted
        if (logData.isHole()) {
            req = getRequestMsg(req.getHeader().toBuilder().setPriority(PriorityLevel.HIGH).build(), req.getPayload());
        }

        final RequestMsg batchProcessorReq = req;
        batchWriter.addTask(BatchWriterOperation.Type.WRITE, batchProcessorReq)
                .thenRunAsync(() -> {
                    dataCache.put(logData.getGlobalAddress(), logData);
                    HeaderMsg responseHeader = getHeaderMsg(batchProcessorReq.getHeader());
                    router.sendResponse(getResponseMsg(responseHeader, getWriteLogResponseMsg()), ctx);
                }, executor)
                .exceptionally(ex -> {
                    handleException(ex, ctx, batchProcessorReq, router);
                    return null;
                });
    }

    /**
     * Services incoming range write calls.
     */
    @RequestHandler(type = PayloadCase.RANGE_WRITE_LOG_REQUEST)
    private void handleRangeWrite(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        List<LogData> range = req.getPayload().getRangeWriteLogRequest().getLogDataList()
                .stream().map(CorfuProtocolLogData::getLogData).collect(Collectors.toList());

        log.debug("handleRangeWrite: Writing {} entries [{}-{}]", range.size(),
                range.get(0).getGlobalAddress(), range.get(range.size() - 1).getGlobalAddress());

        batchWriter.addTask(BatchWriterOperation.Type.RANGE_WRITE, req)
                .thenRun(() -> router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()),
                        getRangeWriteLogResponseMsg()), ctx))
                .exceptionally(ex -> {
                    handleException(ex, ctx, req, router);
                    return null;
                });
    }

    /**
     * Perform a prefix trim (trim log).
     * Here the token is not used to perform the trim as the epoch at which the checkpoint was completed
     * might be old. Hence, we use the msg epoch to perform the trim. This should be safe provided that the
     * trim is performed only on the token provided by the CheckpointWriter which ensures that the checkpoint
     * was persisted. Using any other address to perform a trim can cause data loss.
     */
    @RequestHandler(type = PayloadCase.TRIM_LOG_REQUEST)
    private void handleTrimLog(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        if (log.isDebugEnabled()) {
            log.debug("handleTrimLog[{}]: trimming prefix to {}", req.getHeader().getRequestId(),
                    TextFormat.shortDebugString(req.getPayload().getTrimLogRequest().getAddress()));
        }

        batchWriter.addTask(BatchWriterOperation.Type.PREFIX_TRIM, req)
                .thenRun(() -> {
                    HeaderMsg header = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
                    router.sendResponse(getResponseMsg(header, getTrimLogResponseMsg()), ctx);
                })
                .exceptionally(ex -> {
                    handleException(ex, ctx, req, router);
                    return null;
                });
    }

    @RequestHandler(type = PayloadCase.READ_LOG_REQUEST)
    private void handleRead(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        final boolean cacheable = req.getPayload().getReadLogRequest().getCacheResults();
        final List<Long> addressList = req.getPayload().getReadLogRequest().getAddressList();
        final ReadResponse readResponse = new ReadResponse();

        if (log.isTraceEnabled()) {
            log.trace("handleRead: {}, cacheable: {}", addressList, cacheable);
        }

        for (long address : addressList) {
            try {
                ILogData logData = dataCache.get(address, cacheable);
                if (logData == null) {
                    readResponse.put(address, LogData.getEmpty(address));
                } else {
                    readResponse.put(address, (LogData) logData);
                }
            } catch (DataCorruptionException dce) {
                log.error("handleRead: Data corruption exception while reading addresses {}", addressList, dce);
                router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()), getDataCorruptionErrorMsg(address)), ctx);
                return;
            }
        }

        router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()),
                getReadLogResponseMsg(readResponse.getAddresses())), ctx);
    }

    @RequestHandler(type = PayloadCase.INSPECT_ADDRESSES_REQUEST)
    private void handleInspectAddressesRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        final List<Long> addresses = req.getPayload().getInspectAddressesRequest().getAddressList();
        List<Long> emptyAddresses = new ArrayList<>();

        if (log.isTraceEnabled()) {
            log.trace("handleInspectAddressesRequest[{}]: " +
                    "addresses {}", req.getHeader().getRequestId(), addresses);
        }

        for (long address : addresses) {
            try {
                if (!streamLog.contains(address)) {
                    emptyAddresses.add(address);
                }
            } catch (TrimmedException te) {
                router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()), getTrimmedErrorMsg()), ctx);
                return;
            } catch (DataCorruptionException dce) {
                router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()), getDataCorruptionErrorMsg(address)), ctx);
                return;
            }
        }

        // Note: we reuse the request header as the ignore_cluster_id and
        // ignore_epoch fields are the same in both cases.
        router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()),
                getInspectAddressesResponseMsg(emptyAddresses)), ctx);
    }

    /**
     * Handles requests for known entries in specified range.
     * This is used by state transfer to catch up only the remainder of the segment.
     */
    @RequestHandler(type = PayloadCase.KNOWN_ADDRESS_REQUEST)
    private void handleKnownAddressRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        try {
            Set<Long> knownAddresses = streamLog.getKnownAddressesInRange(
                    req.getPayload().getKnownAddressRequest().getStartRange(),
                    req.getPayload().getKnownAddressRequest().getEndRange());

            // Note: we reuse the request header as the ignore_cluster_id and
            // ignore_epoch fields are the same in both cases.
            router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()),
                    getKnownAddressResponseMsg(knownAddresses)), ctx);
        } catch (Exception e) {
            handleException(e, ctx, req, router);
        }
    }

    @RequestHandler(type = PayloadCase.COMPACT_REQUEST)
    private void handleCompactLogRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        if (log.isDebugEnabled()) {
            log.debug("handleCompactLogRequest: received a compact request {}", TextFormat.shortDebugString(req));
        }

        streamLog.compact();

        // Note: we reuse the request header as the ignore_cluster_id and
        // ignore_epoch fields are the same in both cases.
        router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()), getCompactResponseMsg()), ctx);
    }

    @RequestHandler(type = PayloadCase.FLUSH_CACHE_REQUEST)
    private void handleFlushCacheRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        if (log.isDebugEnabled()) {
            log.debug("handleFlushCacheRequest: received a cache flush request {}", TextFormat.shortDebugString(req));
        }

        dataCache.invalidateAll();

        // Note: we reuse the request header as the ignore_cluster_id and
        // ignore_epoch fields are the same in both cases.
        router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()), getFlushCacheResponseMsg()), ctx);
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
        RequestMsg batchProcessorReq = getRequestMsg(
                HeaderMsg.newBuilder()
                        .setVersion(getDefaultProtocolVersionMsg())
                        .setEpoch(epoch)
                        .setPriority(PriorityLevel.HIGH)
                        .build(),
                getSealRequestMsg(epoch)
        );

        try {
            batchWriter.addTask(BatchWriterOperation.Type.SEAL, batchProcessorReq).join();
        } catch (CompletionException ce) {
            if (ce.getCause() instanceof WrongEpochException) {
                // The BaseServer expects to observe this exception,
                // when it happens, so it needs to be unwrapped
                throw (WrongEpochException) ce.getCause();
            }
        }

        log.info("LogUnit sealServerWithEpoch: sealed and flushed with epoch {}", epoch);
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
    @RequestHandler(type = PayloadCase.RESET_LOG_UNIT_REQUEST)
    private synchronized void handleResetLogUnit(RequestMsg req, ChannelHandlerContext ctx, IServerRouter router) {
        // Check if the reset request is with an epoch greater than the last reset epoch seen to
        // prevent multiple reset in the same epoch. and should be equal to the current router
        // epoch to prevent stale reset requests from wiping out the data.
        if (req.getPayload().getResetLogUnitRequest().getEpoch() <= serverContext.getLogUnitEpochWaterMark() ||
                req.getPayload().getResetLogUnitRequest().getEpoch() != serverContext.getServerEpoch()) {
            log.info("handleResetLogUnit: LogUnit server reset request received but reset already done.");
            router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()), getResetLogUnitResponseMsg()), ctx);
            return;
        }

        serverContext.setLogUnitEpochWaterMark(req.getPayload().getResetLogUnitRequest().getEpoch());

        batchWriter.addTask(BatchWriterOperation.Type.RESET, req)
                .thenRun(() -> {
                    dataCache.invalidateAll();
                    log.info("handleResetLogUnit: LogUnit server reset.");
                    router.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()), getResetLogUnitResponseMsg()), ctx);
                })
                .exceptionally(ex -> {
                    handleException(ex, ctx, req, router);
                    return null;
                });
    }

    /**
     * Shutdown the server.
     */
    @Override
    public void shutdown() {
        log.info("Shutdown LogUnit server. Current epoch: {}, ", serverContext.getServerEpoch());
        super.shutdown();
        executor.shutdown();
        logCleaner.shutdown();
        batchWriter.close();
    }

    @VisibleForTesting
    public LogUnitServerCache getDataCache() {
        return dataCache;
    }

    @VisibleForTesting
    long getMaxCacheSize() {
        return serverContext.getConfiguration().getMaxLogUnitCacheSize();
    }

    @VisibleForTesting
    StreamAddressSpace getStreamAddressSpace(UUID streamID) {
        return streamLog.getStreamsAddressSpace().getAddressMap().get(streamID);
    }

    @VisibleForTesting
    void prefixTrim(long trimAddress) {
        streamLog.prefixTrim(trimAddress);
    }

    /**
     * Log unit server parameters.
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
         * @param conf Server Configuration options
         * @return log unit configuration
         */
        public static LogUnitServerConfig parse(ServerConfiguration conf) {
            double cacheSizeHeapRatio = conf.getLogUnitCacheRatio();

            return LogUnitServerConfig.builder()
                    .cacheSizeHeapRatio(cacheSizeHeapRatio)
                    .maxCacheSize((long) (Runtime.getRuntime().maxMemory() * cacheSizeHeapRatio))
                    .memoryMode(conf.isInMemoryMode())
                    .noVerify(!conf.getVerifyChecksum())
                    .noSync(!conf.getSyncData())
                    .build();
        }
    }

    /**
     * Utility class used by the LogUnit server to initialize its components,
     * including the StreamLog, LogUnitServerCache, BatchProcessor and StreamLogCompaction.
     * This facilitates the injection of mocked objects during unit tests.
     */
    public static class LogUnitServerInitializer {
        public StreamLog buildInMemoryStreamLog() {
            return new InMemoryStreamLog();
        }

        public StreamLog buildStreamLog(@Nonnull ServerContext serverContext) {
            return new StreamLogFiles(serverContext);
        }

        public LogUnitServerCache buildLogUnitServerCache(@Nonnull StreamLog streamLog,
                                                   @Nonnull ServerContext  serverContext) {
            return new LogUnitServerCache(streamLog, serverContext.getConfiguration().getMaxLogUnitCacheSize());
        }

        public BatchProcessor buildBatchProcessor(@Nonnull StreamLog streamLog,
                                           @Nonnull ServerContext serverContext) {
            return new BatchProcessor(streamLog, serverContext.getServerEpoch(), serverContext.getConfiguration().getSyncData());
        }

        public StreamLogCompaction buildStreamLogCompaction(@Nonnull StreamLog streamLog) {
            return new StreamLogCompaction(streamLog, 10, 45, TimeUnit.MINUTES, ServerContext.SHUTDOWN_TIMER);
        }
    }
}
