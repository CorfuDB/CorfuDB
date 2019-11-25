package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelHandlerContext;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.InMemoryStreamLog;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.infrastructure.log.StreamLogParams;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.ExceptionMsg;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.InspectAddressesRequest;
import org.corfudb.protocols.wireprotocol.InspectAddressesResponse;
import org.corfudb.protocols.wireprotocol.KnownAddressRequest;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.LogRecoveryStateResponse;
import org.corfudb.protocols.wireprotocol.LogRecoveryStateWriteMsg;
import org.corfudb.protocols.wireprotocol.MultipleReadRequest;
import org.corfudb.protocols.wireprotocol.MultipleWriteMsg;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.protocols.wireprotocol.ReadRequest;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsRequest;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.WriteRequest;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.LogUnitException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.Utils;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.corfudb.infrastructure.BatchWriterOperation.Type.LOG_ADDRESS_SPACE_QUERY;
import static org.corfudb.infrastructure.BatchWriterOperation.Type.MULTI_GARBAGE_WRITE;
import static org.corfudb.infrastructure.BatchWriterOperation.Type.RANGE_WRITE;
import static org.corfudb.infrastructure.BatchWriterOperation.Type.RECOVERY_STATE_WRITE;
import static org.corfudb.infrastructure.BatchWriterOperation.Type.RESET;
import static org.corfudb.infrastructure.BatchWriterOperation.Type.SEAL;
import static org.corfudb.infrastructure.BatchWriterOperation.Type.TAILS_QUERY;
import static org.corfudb.infrastructure.BatchWriterOperation.Type.WRITE;


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
     * HandlerMethod for this server.
     */
    @Getter
    private final HandlerMethods handler = HandlerMethods.generateHandler(MethodHandles.lookup(), this);

    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent
     * storage.
     */
    private final LogUnitServerCache dataCache;
    private final StreamLog streamLog;
    private final BatchProcessor batchWriter;

    private ExecutorService executor;

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
            StreamLogParams streamLogParams = serverContext.getStreamLogParams();
            streamLog = new StreamLogFiles(streamLogParams, serverContext.getStreamLogDataStore());
            log.info("Log unit server started with stream log parameters: {}", streamLogParams);
        }

        dataCache = new LogUnitServerCache(config, streamLog);
        batchWriter = new BatchProcessor(streamLog, serverContext.getServerEpoch(), !config.isNoSync());

        if (config.enableCompaction) {
            streamLog.startCompactor();
        }
    }


    @Override
    protected void processRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        executor.submit(() -> getHandler().handle(msg, ctx, r));
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
     * Service an incoming query for the committed tail on this log unit server.
     */
    @ServerHandler(type = CorfuMsgType.COMMITTED_TAIL_REQUEST)
    public void handleCommittedTailRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("handleCommittedTailRequest: received a committed log tail request {}", msg);
        r.sendResponse(ctx, msg, CorfuMsgType.COMMITTED_TAIL_RESPONSE.payloadMsg(streamLog.getCommittedTail()));
    }

    /**
     * Service an incoming request to update the current committed tail.
     */
    @ServerHandler(type = CorfuMsgType.UPDATE_COMMITTED_TAIL)
    public void updateCommittedTail(CorfuPayloadMsg<Long> msg,
                                    ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("updateCommittedTail: received request to update committed tail {}", msg);
        streamLog.updateCommittedTail(msg.getPayload());
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    /**
     * Service an incoming message to inform state transfer finished.
     */
    @ServerHandler(type = CorfuMsgType.INFORM_STATE_TRANSFER_FINISHED)
    public void handleInformStateTransferFinished(CorfuMsg msg,
                                                  ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("handleInformStateTransferFinished: received a notice that " +
                "state transfer has finished {}", msg);
        streamLog.setRequireStateTransfer(false);
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
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
        } else {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_SERVER_EXCEPTION.payloadMsg(new ExceptionMsg(ex)));
            throw new LogUnitException(ex);
        }
    }

    /**
     * Service an incoming write request.
     */
    @ServerHandler(type = CorfuMsgType.WRITE)
    public void write(CorfuPayloadMsg<WriteRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        LogData logData = (LogData) msg.getPayload().getData();
        log.debug("log write: type: {}, address: {}, streams: {}", logData.getType(),
                logData.getToken(), logData.getBackpointerMap());

        // Its not clear that making all holes high priority is the right thing to do, but since
        // some reads will block until a hole is filled this is required (i.e. bypass quota checks)
        // because the requirement is to allow reads, but only block writes once the quota is exhausted
        if (logData.isHole()) {
            msg.setPriorityLevel(PriorityLevel.HIGH);
        }

        batchWriter.addTask(WRITE, msg)
                .thenRunAsync(() -> {
                    dataCache.put(msg.getPayload().getGlobalAddress(), logData);
                    r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());
                }, executor)
                .exceptionally(ex -> {
                    handleException(ex, ctx, msg, r);
                    return null;
                });
    }

    /**
     * Services incoming multi write calls.
     */
    @ServerHandler(type = CorfuMsgType.MULTIPLE_WRITE)
    public void multiWrite(CorfuPayloadMsg<MultipleWriteMsg> msg,
                           ChannelHandlerContext ctx, IServerRouter r) {
        List<LogData> entries = msg.getPayload().getEntries();
        log.debug("multiWrite: Writing {} data entries: {}", entries.size());

        batchWriter.addTask(RANGE_WRITE, msg)
                .thenRunAsync(() -> r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg()))
                .exceptionally(ex -> {
                    handleException(ex, ctx, msg, r);
                    return null;
                });
    }

    @ServerHandler(type = CorfuMsgType.MULTIPLE_GARBAGE_WRITE)
    private void multiGarbageWrite(CorfuPayloadMsg<MultipleWriteMsg> msg,
                                   ChannelHandlerContext ctx, IServerRouter r) {
        List<LogData> garbageEntries = msg.getPayload().getEntries();
        log.trace("multiGarbageWrite: Writing {} garbage entries", garbageEntries.size());

        batchWriter.addTask(MULTI_GARBAGE_WRITE, msg)
                .thenRunAsync(() -> r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg()))
                .exceptionally(ex -> {
                    handleException(ex, ctx, msg, r);
                    return null;
                });
    }

    @ServerHandler(type = CorfuMsgType.LOG_RECOVERY_STATES_WRITE)
    private void writeRecoveryStates(CorfuPayloadMsg<LogRecoveryStateWriteMsg> msg,
                                     ChannelHandlerContext ctx, IServerRouter r) {
        LogRecoveryStateWriteMsg recoveryState = msg.getPayload();
        log.debug("writeRecoveryStates: Writing {} log entries, {} garbage entries, " +
                "compaction mark: {}", recoveryState.getLogEntries().size(),
                recoveryState.getLogEntries().size(), recoveryState.getCompactionMark());

        streamLog.updateGlobalCompactionMark(recoveryState.getCompactionMark());
        batchWriter.addTask(RECOVERY_STATE_WRITE, msg)
                .thenRunAsync(() -> r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg()))
                .exceptionally(ex -> {
                    handleException(ex, ctx, msg, r);
                    return null;
                });
    }

    @ServerHandler(type = CorfuMsgType.READ_REQUEST)
    public void read(CorfuPayloadMsg<ReadRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        long address = msg.getPayload().getAddress();
        boolean cacheable = msg.getPayload().isCacheReadResult();
        log.trace("read: {}, cacheable: {}", msg.getPayload().getAddress(), cacheable);

        ReadResponse rr = new ReadResponse();
        try {
            ILogData logData = dataCache.get(address, cacheable);
            if (logData == null) {
                rr.put(address, LogData.getEmpty(address));
            } else {
                rr.put(address, (LogData) logData);
                rr.setCompactionMark(streamLog.getGlobalCompactionMark());
            }
            r.sendResponse(ctx, msg, CorfuMsgType.READ_RESPONSE.payloadMsg(rr));
        } catch (DataCorruptionException e) {
            log.error("Data corruption exception while reading address {}", address, e);
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_CORRUPTION.payloadMsg(address));
        }
    }

    @ServerHandler(type = CorfuMsgType.MULTIPLE_READ_REQUEST)
    public void multiRead(CorfuPayloadMsg<MultipleReadRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        boolean cacheable = msg.getPayload().isCacheReadResult();
        log.trace("multiRead: {}, cacheable: {}", msg.getPayload().getAddresses(), cacheable);

        ReadResponse rr = new ReadResponse();
        try {
            for (long address : msg.getPayload().getAddresses()) {
                ILogData logData = dataCache.get(address, cacheable);
                if (logData == null) {
                    rr.put(address, LogData.getEmpty(address));
                } else {
                    rr.put(address, (LogData) logData);
                }
            }
            rr.setCompactionMark(streamLog.getGlobalCompactionMark());
            r.sendResponse(ctx, msg, CorfuMsgType.READ_RESPONSE.payloadMsg(rr));
        } catch (DataCorruptionException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_CORRUPTION.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.LOG_RECOVERY_STATES_REQUEST)
    public void readRecoveryStates(CorfuPayloadMsg<MultipleReadRequest> msg,
                                   ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("readRecoveryStates: {}", msg.getPayload().getAddresses());

        // Read garbage entries first to avoid losing garbage information since
        // during compaction, garbage entries are pruned after stream entries.
        LogRecoveryStateResponse state = new LogRecoveryStateResponse();
        try {
            for (long address : msg.getPayload().getAddresses()) {
                LogData garbageData = streamLog.readGarbageEntry(address);
                if (garbageData != null) {
                    state.putGarbageData(address, garbageData);
                }

                LogData logData = streamLog.read(address);
                if (logData == null) {
                    state.putLogData(address, LogData.getEmpty(address));
                } else if (!logData.isCompacted()) {
                    // Only return the un-compacted data.
                    state.putLogData(address, logData);
                } else {
                    // Compaction happened after the stream log entries are
                    // read, remove the corresponding garbage entries.
                    state.removeGarbageData(address);
                }
            }
            state.setCompactionMark(streamLog.getGlobalCompactionMark());
            r.sendResponse(ctx, msg, CorfuMsgType.LOG_RECOVERY_STATES_RESPONSE.payloadMsg(state));
        } catch (DataCorruptionException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_CORRUPTION.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.INSPECT_ADDRESSES_REQUEST)
    public void inspectAddresses(CorfuPayloadMsg<InspectAddressesRequest> msg,
                                 ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("inspectAddresses: {}", msg.getPayload().getAddresses());
        InspectAddressesResponse inspectResponse = new InspectAddressesResponse();
        try {
            for (long address : msg.getPayload().getAddresses()) {
                if (!streamLog.contains(address)) {
                    inspectResponse.add(address);
                }
            }
            r.sendResponse(ctx, msg, CorfuMsgType.INSPECT_ADDRESSES_RESPONSE.payloadMsg(inspectResponse));
        } catch (DataCorruptionException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_CORRUPTION.msg());
        }
    }

    /**
     * Handles requests for known entries in specified range.
     * This is used by state transfer to catch up only the remainder of the segment.
     */
    @ServerHandler(type = CorfuMsgType.KNOWN_ADDRESS_REQUEST)
    private void getKnownAddressesInRange(CorfuPayloadMsg<KnownAddressRequest> msg,
                                          ChannelHandlerContext ctx, IServerRouter r) {

        KnownAddressRequest request = msg.getPayload();
        try {
            Set<Long> knownAddresses = streamLog
                    .getKnownAddressesInRange(request.getStartRange(), request.getEndRange());
            r.sendResponse(ctx, msg,
                    CorfuMsgType.KNOWN_ADDRESS_RESPONSE.payloadMsg(knownAddresses));
        } catch (Exception e) {
            handleException(e, ctx, msg, r);
        }
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
        msg.setPriorityLevel(PriorityLevel.HIGH);
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
                    })
                    .exceptionally(ex -> {
                        handleException(ex, ctx, msg, r);
                        return null;
                    });
        } else {
            log.info("LogUnit Server Reset request received but reset already done.");
            r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        }
    }

    /**
     * During normal production operation, log and garbage data are compacted internally and at the server's own pace.
     * This function triggers compaction externally for testing purpose.
     */
    @VisibleForTesting
    @ServerHandler(type = CorfuMsgType.RUN_COMPACTION)
    private void handleCompactionRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.debug("handleCompactionRequest: received a compaction request {}", msg);
        runCompaction();
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    /**
     * Shutdown the server.
     */
    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
        batchWriter.close();
    }

    @VisibleForTesting
    public LogUnitServerCache getDataCache() {
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

    @VisibleForTesting
    StreamAddressSpace getStreamAddressSpace(UUID streamID) {
        return streamLog.getStreamsAddressSpace().getAddressMap().get(streamID);
    }

    @VisibleForTesting
    public void runCompaction() {
        if (config.enableCompaction) {
            throw new IllegalStateException("Cannot manually run compaction since server is " +
                    "started with compaction enabled.");
        }
        if (streamLog instanceof StreamLogFiles) {
            ((StreamLogFiles) streamLog).getCompactor().compact();
        }
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
        private final boolean noSync;
        private final boolean enableCompaction;

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
                    .noSync((Boolean) opts.get("--no-sync"))
                    .enableCompaction(!((Boolean) opts.get("--no-compaction")))
                    .build();
        }
    }
}
