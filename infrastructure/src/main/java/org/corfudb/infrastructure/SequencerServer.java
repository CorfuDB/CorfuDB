package org.corfudb.infrastructure;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.API;
import org.corfudb.runtime.protocol.proto.CorfuProtocol;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.StreamsAddressRequest;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.corfudb.infrastructure.SequencerServerCache.ConflictTxStream;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.SequencerMetrics.SequencerStatus;
import org.corfudb.protocols.wireprotocol.SequencerRecoveryMsg;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenRequest;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TokenType;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.Utils;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.corfudb.protocols.API.*;
import static org.corfudb.protocols.API.getTxResolutionResponse;
import static org.corfudb.runtime.protocol.proto.CorfuProtocol.*;

/**
 * This server implements the sequencer functionality of Corfu.
 *
 * <p>It currently supports a single operation, which is a incoming request:
 *
 * <p>TOKEN - Request the next address.
 *
 * <p>The sequencer server maintains the current tail of the log, the current
 * tail of every stream, and a cache of timestamps of updates on recent
 * conflict-parameters.
 *
 * <p>A token request can be of several sub-types, which are defined in
 * {@link TokenRequest}:
 *
 * <p>{@link TokenRequest::TK_QUERY} - used for only querying the current tail
 * of the log and/or the tails of specific streams
 *
 * <p>{@link TokenRequest::TK_RAW} - reserved for getting a "raw" token in the
 * global log
 *
 * <p>{@link TokenRequest::TK_MULTI_STREAM} - used for logging across one or
 * more streams
 *
 * <p>{@link TokenRequest::TK_TX} - used for reserving an address for transaction
 * commit.
 *
 * <p>The transaction commit is the most sophisticated functaionality of the
 * sequencer. The sequencer reserves an address for the transaction
 * only on if it knows that it can commit.
 *
 * <p>The TK_TX request contains a conflict-set and a write-set. The sequencer
 * checks the conflict-set against the stream-tails and against the
 * conflict-parameters timestamp cache it maintains. If the transaction
 * commits, the sequencer updates the tails of all the streams and the cache
 * of conflict parameters.
 *
 * <p>Created by mwei on 12/8/15.
 */
@Slf4j
public class SequencerServer extends AbstractServer {

    /**
     * Inherit from CorfuServer a server context.
     */
    @Getter
    private final ServerContext serverContext;

    /**
     * - {@link SequencerServer::globalLogTail}:
     * global log first available position (initially, 0).
     */
    @Getter
    private long globalLogTail = Address.getMinAddress();

    private long trimMark = Address.NON_ADDRESS;

    /**
     * - {@link SequencerServer::streamTailToGlobalTailMap}:
     * per streams map to last issued global-log position. used for backpointers.
     */
    private Map<UUID, Long> streamTailToGlobalTailMap = new HashMap<>();

    /**
     * Per streams map and their corresponding address space (an address space is defined by the stream's addresses
     *  and its latest trim mark)
     */
    private Map<UUID, StreamAddressSpace> streamsAddressMap = new HashMap<>();

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * A map to cache the name of timers to avoid creating timer names on each call.
     */
    private final Map<Byte, String> timerNameCache = new HashMap<>();

    /**
     * A map to cache the name of timers to avoid creating timer names on each call.
     */
    private final Map<CorfuProtocol.TokenRequest.TokenRequestType, String> timerNameCacheProto = new HashMap<>();

    /**
     * HandlerMethod for this server.
     */
    @Getter
    private final HandlerMethods handler = HandlerMethods.generateHandler(MethodHandles.lookup(), this);

    @Getter
    private SequencerServerCache cache;

    @Getter
    @Setter
    private volatile long sequencerEpoch = Layout.INVALID_EPOCH;

    /**
     * The lower bound of the consecutive epoch range that this sequencer
     * observes as the primary sequencer. i.e. this sequencer has been the
     * primary sequencer for all the consecutive epochs from this epoch to
     * {@link this#sequencerEpoch}
     */
    @Getter
    private long epochRangeLowerBound = Layout.INVALID_EPOCH;

    private final ExecutorService executor;


    /**
     * Returns a new SequencerServer.
     *
     * @param serverContext context object providing parameters and objects
     */
    public SequencerServer(ServerContext serverContext) {
        this.serverContext = serverContext;
        Config config = Config.parse(serverContext.getServerConfig());

        // Sequencer server is single threaded by current design
        this.executor = Executors.newSingleThreadExecutor(
                new ServerThreadFactory("sequencer-", new ServerThreadFactory.ExceptionHandler()));

        globalLogTail = Address.getMinAddress();
        this.cache = new SequencerServerCache(config.getCacheSize(), globalLogTail - 1);

        // Remove this after Protobuf for RPC Completion
        setUpTimerNameCache();

        setUpTimerNameCacheProto();
    }

    // Remove this after Protobuf for RPC Completion
    @Override
    protected void processRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        executor.submit(() -> getHandler().handle(msg, ctx, r));
    }

    @Override
    protected void processRequest(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        executor.submit(() -> getHandlerMethods().handle(req, ctx, r));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
    }

    // Remove this after Protobuf for RPC Completion
    @Override
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        if (getState() != ServerState.READY){
            return false;
        }

        if ((sequencerEpoch != serverContext.getServerEpoch())
                && (!msg.getMsgType().equals(CorfuMsgType.BOOTSTRAP_SEQUENCER))) {
            log.warn("Rejecting msg at sequencer : sequencerStateEpoch:{}, serverEpoch:{}, "
                    + "msg:{}", sequencerEpoch, serverContext.getServerEpoch(), msg);
            return false;
        }
        return true;
    }

    @Override
    public boolean isServerReadyToHandleReq(Header header) {
        if (getState() != ServerState.READY){
            return false;
        }

        if ((sequencerEpoch != serverContext.getServerEpoch())
                && (!header.getType().equals(MessageType.BOOTSTRAP_SEQUENCER))) {
            log.warn("Rejecting msg at sequencer : sequencerStateEpoch:{}, serverEpoch:{}, "
                    + "header:{}", sequencerEpoch, serverContext.getServerEpoch(), header);
            return false;
        }
        return true;
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * Initialized the HashMap with the name of timers for different types of requests
     */
    private void setUpTimerNameCache() {
        timerNameCache.put(TokenRequest.TK_QUERY, CorfuComponent.INFRA_SEQUENCER + "query-token");
        timerNameCache.put(TokenRequest.TK_RAW, CorfuComponent.INFRA_SEQUENCER + "raw-token");
        timerNameCache.put(TokenRequest.TK_MULTI_STREAM, CorfuComponent.INFRA_SEQUENCER + "multi-stream-token");
        timerNameCache.put(TokenRequest.TK_TX, CorfuComponent.INFRA_SEQUENCER + "tx-token");
    }

    /**
     * Initialized the HashMap with the name of timers for different types of requests
     */
    private void setUpTimerNameCacheProto() {
        timerNameCacheProto.put(CorfuProtocol.TokenRequest.TokenRequestType.TK_QUERY,
                CorfuComponent.INFRA_SEQUENCER + "query-token");
        timerNameCacheProto.put(CorfuProtocol.TokenRequest.TokenRequestType.TK_RAW,
                CorfuComponent.INFRA_SEQUENCER + "raw-token");
        timerNameCacheProto.put(CorfuProtocol.TokenRequest.TokenRequestType.TK_MULTI_STREAM,
                CorfuComponent.INFRA_SEQUENCER + "multi-stream-token");
        timerNameCacheProto.put(CorfuProtocol.TokenRequest.TokenRequestType.TK_TX,
                CorfuComponent.INFRA_SEQUENCER + "tx-token");
    }

    /**
     * Checks if an epoch is within a consecutive closed range
     * [{@link this#epochRangeLowerBound}, {@link this#sequencerEpoch}].
     *
     * This sequencer serves as the primary sequencer for all the
     * consecutive epochs in this range.
     *
     * @param epoch epoch to verify
     * @return true if this epoch is in the range, false otherwise
     */
    private boolean isEpochInRange(long epoch) {
        return epoch >= epochRangeLowerBound && epoch <= sequencerEpoch;
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * If the request submits a timestamp (a global offset) that is less than one of the
     * global offsets of a streams specified in the request, then abort; otherwise commit.
     *
     * @param txInfo info provided by corfuRuntime for conflict resolution:
     *               - timestamp : the snapshot (global) offset that this TX reads
     *               - conflictSet: conflict set of the txn.
     *               if any conflict-param (or stream, if empty) in this set has a later
     *               timestamp than the snapshot, abort
     * @return an instance of transaction resolution response
     */
    private TxResolutionResponse txnCanCommit(TxResolutionInfo txInfo) {
        log.trace("Commit-req[{}]", txInfo);
        final Token txSnapshotTimestamp = txInfo.getSnapshotTimestamp();

        // A transaction can start with a timestamp issued from a previous
        // epoch, so we need to reject transactions that have a snapshot
        // timestamp's epoch less than the epochRangeLowerBound since we are
        // sure this sequencer is always the primary sequencer after this epoch.
        long txSnapshotEpoch = txSnapshotTimestamp.getEpoch();
        if (!isEpochInRange(txSnapshotEpoch)) {
            log.debug("ABORT[{}] snapshot-ts[{}] current epoch[{}] lower bound[{}]",
                    txInfo, txSnapshotTimestamp, sequencerEpoch, epochRangeLowerBound);
            return new TxResolutionResponse(TokenType.TX_ABORT_NEWSEQ);
        }

        if (txSnapshotTimestamp.getSequence() < trimMark) {
            log.debug("ABORT[{}] snapshot-ts[{}] trimMark-ts[{}]", txInfo, txSnapshotTimestamp, trimMark);
            return new TxResolutionResponse(TokenType.TX_ABORT_SEQ_TRIM);
        }

        for (Map.Entry<UUID, Set<byte[]>> conflictStream : txInfo.getConflictSet().entrySet()) {

            // if conflict-parameters are present, check for conflict based on conflict-parameter
            // updates
            Set<byte[]> conflictParamSet = conflictStream.getValue();
            //check for conflict based on streams updates
            if (conflictParamSet == null || conflictParamSet.isEmpty()) {
                UUID streamId = conflictStream.getKey();
                Long sequence = streamTailToGlobalTailMap.get(streamId);
                if (sequence != null && sequence > txSnapshotTimestamp.getSequence()) {
                    log.debug("ABORT[{}] conflict-stream[{}](ts={})", txInfo, Utils.toReadableId(streamId), sequence);
                    return new TxResolutionResponse(TokenType.TX_ABORT_CONFLICT);
                }
                continue;
            }

            // for each key pair, check for conflict; if not present, check against the wildcard
            for (byte[] conflictParam : conflictParamSet) {

                Long keyAddress = cache.get(new ConflictTxStream(conflictStream.getKey(),
                        conflictParam, Address.NON_ADDRESS));

                log.trace("Commit-ck[{}] conflict-key[{}](ts={})", txInfo, conflictParam, keyAddress);

                if (keyAddress > txSnapshotTimestamp.getSequence()) {
                    log.debug("ABORT[{}] conflict-key[{}](ts={})", txInfo, conflictParam, keyAddress);
                    return new TxResolutionResponse(
                            TokenType.TX_ABORT_CONFLICT,
                            keyAddress,
                            conflictParam,
                            conflictStream.getKey()
                    );
                }

                // The maxConflictNewSequencer is modified whenever a server is elected
                // as the 'new' sequencer, we immediately set its value to the max timestamp
                // evicted from the cache at that time. If a txSnapshotTimestamp falls
                // under this threshold we can report that the cause of abort is due to
                // a NEW_SEQUENCER (not able to hold these in its cache).
                long maxConflictNewSequencer = cache.getMaxConflictNewSequencer();
                if (txSnapshotTimestamp.getSequence() < maxConflictNewSequencer) {
                    log.debug("ABORT[{}] snapshot-ts[{}] WILDCARD New Sequencer ts=[{}]",
                            txInfo, txSnapshotTimestamp, maxConflictNewSequencer);
                    return new TxResolutionResponse(TokenType.TX_ABORT_NEWSEQ);
                }

                // If the txSnapshotTimestamp did not fall under the new sequencer threshold
                // but it does fall under the latest evicted timestamp we report the cause of
                // abort as SEQUENCER_OVERFLOW
                long maxConflictWildcard = cache.getMaxConflictWildcard();
                if (txSnapshotTimestamp.getSequence() < maxConflictWildcard) {
                    log.debug("ABORT[{}] snapshot-ts[{}] WILDCARD ts=[{}]",
                            txInfo, txSnapshotTimestamp, maxConflictWildcard);
                    return new TxResolutionResponse(TokenType.TX_ABORT_SEQ_OVERFLOW);
                }
            }
        }

        return new TxResolutionResponse(TokenType.NORMAL);
    }

    /**
     * If the request submits a timestamp (a global offset) that is less than one of the
     * global offsets of a streams specified in the request, then abort; otherwise commit.
     *
     * @param txInfo info provided by corfuRuntime for conflict resolution:
     *               - timestamp : the snapshot (global) offset that this TX reads
     *               - conflictSet: conflict set of the txn.
     *               if any conflict-param (or stream, if empty) in this set has a later
     *               timestamp than the snapshot, abort
     * @return an instance of transaction resolution response
     */
    private CorfuProtocol.TxResolutionResponse txnCanCommit(CorfuProtocol.TxResolutionInfo txInfo) {
        log.trace("Commit-req[{}]", txInfo);
        final CorfuProtocol.Token txSnapshotTimestamp = txInfo.getSnapshotTimestamp();

        // A transaction can start with a timestamp issued from a previous
        // epoch, so we need to reject transactions that have a snapshot
        // timestamp's epoch less than the epochRangeLowerBound since we are
        // sure this sequencer is always the primary sequencer after this epoch.
        long txSnapshotEpoch = txSnapshotTimestamp.getEpoch();
        if (!isEpochInRange(txSnapshotEpoch)) {
            log.debug("ABORT[{}] snapshot-ts[{}] current epoch[{}] lower bound[{}]",
                    txInfo, txSnapshotTimestamp, sequencerEpoch, epochRangeLowerBound);
            return getTxResolutionResponse(CorfuProtocol.TokenType.TX_ABORT_NEWSEQ);
        }

        if (txSnapshotTimestamp.getSequence() < trimMark) {
            log.debug("ABORT[{}] snapshot-ts[{}] trimMark-ts[{}]", txInfo, txSnapshotTimestamp, trimMark);
            return getTxResolutionResponse(CorfuProtocol.TokenType.TX_ABORT_SEQ_TRIM);
        }

        for (UUIDToListOfBytesPair conflictStream : txInfo.getConflictSet().getEntriesList()) {

            // if conflict-parameters are present, check for conflict based on conflict-parameter
            // updates
            List<ByteString> conflictParamSet = conflictStream.getValueList();
            //check for conflict based on streams updates
            if (conflictParamSet == null || conflictParamSet.isEmpty()) {
                CorfuProtocol.UUID streamId = conflictStream.getKey();
                Long sequence = streamTailToGlobalTailMap.get(getJavaUUID(streamId));
                if (sequence != null && sequence > txSnapshotTimestamp.getSequence()) {
                    log.debug("ABORT[{}] conflict-stream[{}](ts={})", txInfo, Utils.toReadableId(getJavaUUID(streamId)), sequence);
                    return getTxResolutionResponse(CorfuProtocol.TokenType.TX_ABORT_CONFLICT);
                }
                continue;
            }

            // for each key pair, check for conflict; if not present, check against the wildcard
            for (ByteString conflictParam : conflictParamSet) {

                Long keyAddress = cache.get(new ConflictTxStream(getJavaUUID(conflictStream.getKey()),
                        conflictParam.toByteArray(), Address.NON_ADDRESS));

                log.trace("Commit-ck[{}] conflict-key[{}](ts={})", txInfo, conflictParam, keyAddress);

                if (keyAddress > txSnapshotTimestamp.getSequence()) {
                    log.debug("ABORT[{}] conflict-key[{}](ts={})", txInfo, conflictParam, keyAddress);
                    return getTxResolutionResponse(
                            CorfuProtocol.TokenType.TX_ABORT_CONFLICT,
                            keyAddress,
                            conflictParam,
                            conflictStream.getKey()
                    );
                }

                // The maxConflictNewSequencer is modified whenever a server is elected
                // as the 'new' sequencer, we immediately set its value to the max timestamp
                // evicted from the cache at that time. If a txSnapshotTimestamp falls
                // under this threshold we can report that the cause of abort is due to
                // a NEW_SEQUENCER (not able to hold these in its cache).
                long maxConflictNewSequencer = cache.getMaxConflictNewSequencer();
                if (txSnapshotTimestamp.getSequence() < maxConflictNewSequencer) {
                    log.debug("ABORT[{}] snapshot-ts[{}] WILDCARD New Sequencer ts=[{}]",
                            txInfo, txSnapshotTimestamp, maxConflictNewSequencer);
                    return getTxResolutionResponse(CorfuProtocol.TokenType.TX_ABORT_NEWSEQ);
                }

                // If the txSnapshotTimestamp did not fall under the new sequencer threshold
                // but it does fall under the latest evicted timestamp we report the cause of
                // abort as SEQUENCER_OVERFLOW
                long maxConflictWildcard = cache.getMaxConflictWildcard();
                if (txSnapshotTimestamp.getSequence() < maxConflictWildcard) {
                    log.debug("ABORT[{}] snapshot-ts[{}] WILDCARD ts=[{}]",
                            txInfo, txSnapshotTimestamp, maxConflictWildcard);
                    return getTxResolutionResponse(CorfuProtocol.TokenType.TX_ABORT_SEQ_OVERFLOW);
                }
            }
        }

        return getTxResolutionResponse(CorfuProtocol.TokenType.TX_NORMAL);
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * Service a query request.
     *
     * <p>This returns information about the tail of the
     * log and/or streams without changing/allocating anything.
     *
     * @param msg corfu message containing token query
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleTokenQuery(CorfuPayloadMsg<TokenRequest> msg,
                                  ChannelHandlerContext ctx, IServerRouter r) {
        TokenRequest req = msg.getPayload();
        List<UUID> streams = req.getStreams();
        Map<UUID, Long> streamTails;
        Token token;
        if (req.getStreams().isEmpty()) {
            // Global tail query
            token = new Token(sequencerEpoch, globalLogTail - 1);
            streamTails = Collections.emptyMap();
        } else {
            // multiple or single stream query, the token is populated with the global tail
            // and the tail queries are stored in streamTails
            token = new Token(sequencerEpoch, globalLogTail - 1);
            streamTails = new HashMap<>(streams.size());
            for (UUID stream : streams) {
                streamTails.put(stream, streamTailToGlobalTailMap.getOrDefault(stream, Address.NON_EXIST));
            }
        }

        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(new TokenResponse(
                TokenType.NORMAL, TokenResponse.NO_CONFLICT_KEY,
                TokenResponse.NO_CONFLICT_STREAM, token, Collections.emptyMap(), streamTails)));

    }

    /**
     * Service a query request.
     *
     * <p>This returns information about the tail of the
     * log and/or streams without changing/allocating anything.
     *
     * @param req corfu message containing token query
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleTokenQuery(Request req,
                                  ChannelHandlerContext ctx, IRequestRouter r) {
        final CorfuProtocol.TokenRequest tokenRequest = req.getTokenRequest();
        List<CorfuProtocol.UUID> streams = tokenRequest.getStreamsList();
        Map<UUID, Long> streamTails;
        CorfuProtocol.Token token;
        if (tokenRequest.getStreamsList().isEmpty()) {
            // Global tail query
            token = getToken(sequencerEpoch, globalLogTail - 1);
            streamTails = Collections.emptyMap();
        } else {
            // multiple or single stream query, the token is populated with the global tail
            // and the tail queries are stored in streamTails
            token = getToken(sequencerEpoch, globalLogTail - 1);
            streamTails = new HashMap<>(streams.size());
            for (CorfuProtocol.UUID stream : streams) {
                streamTails.put(getJavaUUID(stream),
                        streamTailToGlobalTailMap.getOrDefault(getJavaUUID(stream), Address.NON_EXIST));
            }
        }

        Header responseHeader = generateResponseHeader(req.getHeader(),
                req.getHeader().getIgnoreClusterId(), req.getHeader().getIgnoreEpoch());
        CorfuProtocol.TokenResponse tokenResponse = getTokenResponse(
                CorfuProtocol.TokenType.TX_NORMAL,
                ByteString.copyFrom(TOKEN_RESPONSE_NO_CONFLICT_KEY),
                getUUID(TOKEN_RESPONSE_NO_CONFLICT_STREAM),
                token,
                UUIDToLongMap.getDefaultInstance(),
                getProtoUUIDToLongMap(streamTails));
        Response response = getTokenResponse(responseHeader, tokenResponse);
        r.sendResponse(response, ctx);
    }

    // Remove this after Protobuf for RPC Completion
    @ServerHandler(type = CorfuMsgType.SEQUENCER_TRIM_REQ)
    public void trimCache(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.info("trimCache: Starting cache eviction");
        if (trimMark < msg.getPayload()) {
            // Advance the trim mark, if the new trim request has a higher trim mark.
            trimMark = msg.getPayload();
            cache.invalidateUpTo(trimMark);

            // Remove trimmed addresses from each address map and set new trim mark
            for(StreamAddressSpace streamAddressSpace : streamsAddressMap.values()) {
                streamAddressSpace.trim(trimMark);
            }
        }

        log.debug("trimCache: global trim {}, streamsAddressSpace {}", trimMark, streamsAddressMap);

        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @RequestHandler(type = CorfuProtocol.MessageType.SEQUENCER_TRIM)
    public void trimCache(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        log.info("trimCache: Starting cache eviction");
        if (trimMark < req.getSequencerTrimRequest().getTrimMark()) {
            // Advance the trim mark, if the new trim request has a higher trim mark.
            trimMark = req.getSequencerTrimRequest().getTrimMark();
            cache.invalidateUpTo(trimMark);

            // Remove trimmed addresses from each address map and set new trim mark
            for(StreamAddressSpace streamAddressSpace : streamsAddressMap.values()) {
                streamAddressSpace.trim(trimMark);
            }
        }

        log.debug("trimCache: global trim {}, streamsAddressSpace {}", trimMark, streamsAddressMap);

        Header responseHeader = generateResponseHeader(req.getHeader(),
                req.getHeader().getIgnoreClusterId(), req.getHeader().getIgnoreEpoch());
        Response response = getSequencerTrimResponse(responseHeader);
        r.sendResponse(response, ctx);
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * Service an incoming request to reset the sequencer.
     */
    @ServerHandler(type = CorfuMsgType.BOOTSTRAP_SEQUENCER)
    public void resetServer(CorfuPayloadMsg<SequencerRecoveryMsg> msg,
                                         ChannelHandlerContext ctx, IServerRouter r) {
        log.info("Reset sequencer server.");
        final Map<UUID, StreamAddressSpace> addressSpaceMap = msg.getPayload().getStreamsAddressMap();
        final long bootstrapMsgEpoch = msg.getPayload().getSequencerEpoch();

        // Boolean flag to denote whether this bootstrap message is just updating an existing
        // primary sequencer with the new epoch (if set to true) or bootstrapping a currently
        // NOT_READY sequencer.
        final boolean bootstrapWithoutTailsUpdate = msg.getPayload()
                .getBootstrapWithoutTailsUpdate();

        // If sequencerEpoch is -1 (startup) OR bootstrapMsgEpoch is not the consecutive epoch of
        // the sequencerEpoch then the sequencer should not accept bootstrapWithoutTailsUpdate
        // bootstrap messages.
        if (bootstrapWithoutTailsUpdate
                && (sequencerEpoch == Layout.INVALID_EPOCH || bootstrapMsgEpoch != sequencerEpoch + 1)) {

            log.warn("Cannot update existing sequencer. Require full bootstrap. SequencerEpoch : {}, MsgEpoch : {}",
                    sequencerEpoch, bootstrapMsgEpoch
            );

            r.sendResponse(ctx, msg, CorfuMsgType.NACK.msg());
            return;
        }

        // Stale bootstrap request should be discarded.
        if (serverContext.getSequencerEpoch() >= bootstrapMsgEpoch) {
            log.info("Sequencer already bootstrapped at epoch {}. Discarding bootstrap request with epoch {}",
                    sequencerEpoch, bootstrapMsgEpoch
            );

            r.sendResponse(ctx, msg, CorfuMsgType.NACK.msg());
            return;
        }

        // If the sequencer is reset, then we can't know when was
        // the latest update to any stream or conflict parameter.
        // hence, we will accept any bootstrap message with a higher epoch and forget any existing
        // token count or stream tails.
        //
        // Note, this is correct, but conservative (may lead to false abort).
        // It is necessary because we reset the sequencer.
        if (!bootstrapWithoutTailsUpdate) {
            globalLogTail = msg.getPayload().getGlobalTail();
            cache = new SequencerServerCache(cache.getCacheSize(), globalLogTail - 1);
            // Clear the existing map as it could have been populated by an earlier reset.
            streamTailToGlobalTailMap = new HashMap<>();

            // Set tail for every stream
            for(Map.Entry<UUID, StreamAddressSpace> streamAddressSpace : addressSpaceMap.entrySet()) {
                Long streamTail = streamAddressSpace.getValue().getTail();
                log.trace("On Sequencer reset, tail for stream {} set to {}", streamAddressSpace.getKey(), streamTail);
                streamTailToGlobalTailMap.put(streamAddressSpace.getKey(), streamTail);
            }

            // Reset streams address map
            this.streamsAddressMap = new HashMap<>();
            this.streamsAddressMap.putAll(addressSpaceMap);

            for (Map.Entry<UUID, StreamAddressSpace> streamAddressSpace : this.streamsAddressMap.entrySet()) {
                log.info("Stream[{}] set to last trimmed address {} and {} addresses in the range [{}-{}], " +
                                "on sequencer reset.",
                        Utils.toReadableId(streamAddressSpace.getKey()),
                        streamAddressSpace.getValue().getTrimMark(),
                        streamAddressSpace.getValue().getAddressMap().getLongCardinality(),
                        streamAddressSpace.getValue().getLowestAddress(),
                        streamAddressSpace.getValue().getHighestAddress());
                if (log.isTraceEnabled()) {
                    log.trace("Stream[{}] address map on sequencer reset: {}",
                            Utils.toReadableId(streamAddressSpace.getKey()), streamAddressSpace.getValue().getAddressMap());
                }
            }
        }

        // Update epochRangeLowerBound if the bootstrap epoch is not consecutive.
        if (epochRangeLowerBound == Layout.INVALID_EPOCH || bootstrapMsgEpoch != sequencerEpoch + 1) {
            epochRangeLowerBound = bootstrapMsgEpoch;
        }

        // Mark the sequencer as ready after the tails have been populated.
        sequencerEpoch = bootstrapMsgEpoch;
        serverContext.setSequencerEpoch(bootstrapMsgEpoch);

        log.info("Sequencer reset with token = {}, size {} streamTailToGlobalTailMap = {}, sequencerEpoch = {}",
                globalLogTail, streamTailToGlobalTailMap.size(), streamTailToGlobalTailMap, sequencerEpoch);

        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    /**
     * Service an incoming request to reset the sequencer.
     */
    @RequestHandler(type = CorfuProtocol.MessageType.BOOTSTRAP_SEQUENCER)
    public void resetServer(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        log.info("Reset sequencer server.");
        final Map<UUID, StreamAddressSpace> addressSpaceMap = new HashMap<>();

        // Converting from addressSpaceProtoMap to java addressSpaceMap as
        // this.streamsAddressMap needs java objects in putAll() method call
        final UUIDToStreamAddressMap addressSpaceProtoMap = req.getBootstrapSequencerRequest()
                                    .getStreamsAddressMap();
        addressSpaceProtoMap.getEntriesList().forEach(uuidToStreamAddressPair -> {

            Roaring64NavigableMap roaring64NavigableMap = new Roaring64NavigableMap();
            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
                    uuidToStreamAddressPair.getValue().getAddressMap().toByteArray());
            final DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
            try {
                roaring64NavigableMap.deserialize(dataInputStream);
            } catch (IOException e) {
                log.error("resetServer: error while deserializing roaring64NavigableMap");
            }
            addressSpaceMap.put(getJavaUUID(uuidToStreamAddressPair.getKey()),
                    new StreamAddressSpace(uuidToStreamAddressPair.getValue().getTrimMark(),
                            roaring64NavigableMap));

        });

        final long bootstrapMsgEpoch = req.getBootstrapSequencerRequest().getSequencerEpoch();

        // Boolean flag to denote whether this bootstrap message is just updating an existing
        // primary sequencer with the new epoch (if set to true) or bootstrapping a currently
        // NOT_READY sequencer.
        final boolean bootstrapWithoutTailsUpdate = req.getBootstrapSequencerRequest()
                .getBootstrapWithoutTailsUpdate();

        // If sequencerEpoch is -1 (startup) OR bootstrapMsgEpoch is not the consecutive epoch of
        // the sequencerEpoch then the sequencer should not accept bootstrapWithoutTailsUpdate
        // bootstrap messages.
        if (bootstrapWithoutTailsUpdate
                && (sequencerEpoch == Layout.INVALID_EPOCH || bootstrapMsgEpoch != sequencerEpoch + 1)) {

            log.warn("Cannot update existing sequencer. Require full bootstrap. SequencerEpoch : {}, MsgEpoch : {}",
                    sequencerEpoch, bootstrapMsgEpoch
            );

            Header responseHeader = API.generateResponseHeader(req.getHeader(),
                    req.getHeader().getIgnoreClusterId(), req.getHeader().getIgnoreEpoch());
            r.sendWrongEpochError(responseHeader, ctx);
            return;
        }

        // Stale bootstrap request should be discarded.
        if (serverContext.getSequencerEpoch() >= bootstrapMsgEpoch) {
            log.info("Sequencer already bootstrapped at epoch {}. Discarding bootstrap request with epoch {}",
                    sequencerEpoch, bootstrapMsgEpoch
            );

            Header responseHeader = API.generateResponseHeader(req.getHeader(),
                    req.getHeader().getIgnoreClusterId(), req.getHeader().getIgnoreEpoch());
            r.sendWrongEpochError(responseHeader, ctx);
            return;
        }

        // If the sequencer is reset, then we can't know when was
        // the latest update to any stream or conflict parameter.
        // hence, we will accept any bootstrap message with a higher epoch and forget any existing
        // token count or stream tails.
        //
        // Note, this is correct, but conservative (may lead to false abort).
        // It is necessary because we reset the sequencer.
        if (!bootstrapWithoutTailsUpdate) {
            globalLogTail = req.getBootstrapSequencerRequest().getGlobalTail();
            cache = new SequencerServerCache(cache.getCacheSize(), globalLogTail - 1);
            // Clear the existing map as it could have been populated by an earlier reset.
            streamTailToGlobalTailMap = new HashMap<>();

            // Set tail for every stream
            for(Map.Entry<UUID, StreamAddressSpace> streamAddressSpace : addressSpaceMap.entrySet()) {
                Long streamTail = streamAddressSpace.getValue().getTail();
                log.trace("On Sequencer reset, tail for stream {} set to {}", streamAddressSpace.getKey(), streamTail);
                streamTailToGlobalTailMap.put(streamAddressSpace.getKey(), streamTail);
            }

            // Reset streams address map
            this.streamsAddressMap = new HashMap<>();
            this.streamsAddressMap.putAll(addressSpaceMap);

            for (Map.Entry<UUID, StreamAddressSpace> streamAddressSpace : this.streamsAddressMap.entrySet()) {
                log.info("Stream[{}] set to last trimmed address {} and {} addresses in the range [{}-{}], " +
                                "on sequencer reset.",
                        Utils.toReadableId(streamAddressSpace.getKey()),
                        streamAddressSpace.getValue().getTrimMark(),
                        streamAddressSpace.getValue().getAddressMap().getLongCardinality(),
                        streamAddressSpace.getValue().getLowestAddress(),
                        streamAddressSpace.getValue().getHighestAddress());
                if (log.isTraceEnabled()) {
                    log.trace("Stream[{}] address map on sequencer reset: {}",
                            Utils.toReadableId(streamAddressSpace.getKey()), streamAddressSpace.getValue().getAddressMap());
                }
            }
        }

        // Update epochRangeLowerBound if the bootstrap epoch is not consecutive.
        if (epochRangeLowerBound == Layout.INVALID_EPOCH || bootstrapMsgEpoch != sequencerEpoch + 1) {
            epochRangeLowerBound = bootstrapMsgEpoch;
        }

        // Mark the sequencer as ready after the tails have been populated.
        sequencerEpoch = bootstrapMsgEpoch;
        serverContext.setSequencerEpoch(bootstrapMsgEpoch);

        log.info("Sequencer reset with token = {}, size {} streamTailToGlobalTailMap = {}, sequencerEpoch = {}",
                globalLogTail, streamTailToGlobalTailMap.size(), streamTailToGlobalTailMap, sequencerEpoch);

        Header responseHeader = API.generateResponseHeader(req.getHeader(),
                req.getHeader().getIgnoreClusterId(), req.getHeader().getIgnoreEpoch());
        Response response = API.getBootstrapSequencerResponse(responseHeader);
        r.sendResponse(response, ctx);
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * Service an incoming metrics request with the metrics response.
     */
    @ServerHandler(type = CorfuMsgType.SEQUENCER_METRICS_REQUEST)
    public void handleMetricsRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        // Sequencer Ready flag is set to true as this message will be responded to only if the
        // sequencer is in a ready state.
        SequencerMetrics sequencerMetrics = new SequencerMetrics(SequencerStatus.READY);
        r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.SEQUENCER_METRICS_RESPONSE, sequencerMetrics));
    }

    /**
     * Service an incoming metrics request with the metrics response.
     */
    @RequestHandler(type = CorfuProtocol.MessageType.SEQUENCER_METRICS)
    public void handleMetricsRequest(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        // Sequencer Ready flag is set to true as this message will be responded to only if the
        // sequencer is in a ready state.
        CorfuProtocol.SequencerMetrics sequencerMetrics =
                getSequencerMetrics(CorfuProtocol.SequencerMetrics.SequencerStatus.READY);

        Header responseHeader = API.generateResponseHeader(req.getHeader(),
                req.getHeader().getIgnoreClusterId(), req.getHeader().getIgnoreEpoch());
        Response response = API.getSequencerMetricsResponse(responseHeader, sequencerMetrics);
        r.sendResponse(response, ctx);
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * Service an incoming token request.
     */
    @ServerHandler(type = CorfuMsgType.TOKEN_REQ)
    public void tokenRequest(CorfuPayloadMsg<TokenRequest> msg,
                             ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("Token request. Msg: {}", msg);

        TokenRequest req = msg.getPayload();
        final Timer timer = getTimer(req.getReqType());

        // dispatch request handler according to request type while collecting the timer metrics
        try (Timer.Context context = MetricsUtils.getConditionalContext(timer)) {
            switch (req.getReqType()) {
                case TokenRequest.TK_QUERY:
                    handleTokenQuery(msg, ctx, r);
                    return;

                case TokenRequest.TK_RAW:
                    handleRawToken(msg, ctx, r);
                    return;

                case TokenRequest.TK_TX:
                    handleTxToken(msg, ctx, r);
                    return;

                default:
                    handleAllocation(msg, ctx, r);
                    return;
            }
        }
    }

    /**
     * Service an incoming token request.
     */
    @RequestHandler(type = CorfuProtocol.MessageType.TOKEN)
    public void tokenRequest(Request req,
                             ChannelHandlerContext ctx, IRequestRouter r) {
        log.trace("Token request. Msg: {}", req);

        final CorfuProtocol.TokenRequest tokenRequest = req.getTokenRequest();
        final Timer timer = getTimer(tokenRequest.getRequestType());

        // dispatch request handler according to request type while collecting the timer metrics
        try (Timer.Context context = MetricsUtils.getConditionalContext(timer)) {
            switch (tokenRequest.getRequestType()) {
                case TK_QUERY:
                    handleTokenQuery(req, ctx, r);
                    return;

                case TK_RAW:
                    handleRawToken(req, ctx, r);
                    return;

                case TK_TX:
                    handleTxToken(req, ctx, r);
                    return;

                default:
                    handleAllocation(req, ctx, r);
            }
        }
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * Return a timer based on the type of request. It will take the name from the cache
     * initialized at construction of the sequencer server to avoid String concatenation.
     *
     * @param reqType type of a {@link TokenRequest} instance
     * @return an instance {@link Timer} corresponding to the provided {@param reqType}
     */
    private Timer getTimer(byte reqType) {
        final String timerName = timerNameCache.getOrDefault(reqType, CorfuComponent.INFRA_SEQUENCER + "unknown");
        return ServerContext.getMetrics().timer(timerName);
    }

    /**
     * Return a timer based on the type of request. It will take the name from the cache
     * initialized at construction of the sequencer server to avoid String concatenation.
     *
     * @param reqType type of a {@link TokenRequest} instance
     * @return an instance {@link Timer} corresponding to the provided {@param reqType}
     */
    private Timer getTimer(CorfuProtocol.TokenRequest.TokenRequestType reqType) {
        final String timerName = timerNameCacheProto.getOrDefault(reqType,
                CorfuComponent.INFRA_SEQUENCER + "unknown");
        return ServerContext.getMetrics().timer(timerName);
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * this method serves log-tokens for a raw log implementation.
     * it simply extends the global log tail and returns the global-log token
     *
     * @param msg corfu message containing raw token
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleRawToken(CorfuPayloadMsg<TokenRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        final TokenRequest req = msg.getPayload();

        // The global tail points to an open slot, not the last written slot,
        // so return the new token with current global tail and then update it.
        Token token = new Token(sequencerEpoch, globalLogTail);
        globalLogTail += req.getNumTokens();
        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                new TokenResponse(token, Collections.emptyMap())));
    }

    /**
     * this method serves log-tokens for a raw log implementation.
     * it simply extends the global log tail and returns the global-log token
     *
     * @param req corfu message containing raw token
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleRawToken(Request req,
                                ChannelHandlerContext ctx, IRequestRouter r) {
        final CorfuProtocol.TokenRequest tokenRequest = req.getTokenRequest();

        // The global tail points to an open slot, not the last written slot,
        // so return the new token with current global tail and then update it.
        CorfuProtocol.Token token = getToken(sequencerEpoch, globalLogTail);
        globalLogTail += tokenRequest.getNumTokens();

        Header responseHeader = API.generateResponseHeader(req.getHeader(),
                req.getHeader().getIgnoreClusterId(), req.getHeader().getIgnoreEpoch());
        CorfuProtocol.TokenResponse tokenResponse = getTokenResponse(
                token,
                UUIDToLongMap.getDefaultInstance());
        Response response = getTokenResponse(responseHeader, tokenResponse);
        r.sendResponse(response, ctx);
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * this method serves token-requests for transaction-commit entries.
     *
     * <p>it checks if the transaction can commit.
     * - if the transaction must abort,
     * then a 'error token' containing an Address.ABORTED address is returned.
     * - if the transaction may commit,
     * then a normal allocation of log position(s) is pursued.
     *
     * @param msg corfu message containing transaction token
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleTxToken(CorfuPayloadMsg<TokenRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        final TokenRequest req = msg.getPayload();

        // in the TK_TX request type, the sequencer is utilized for transaction conflict-resolution.
        // Token allocation is conditioned on commit.
        // First, we check if the transaction can commit.
        TxResolutionResponse txResolutionResponse = txnCanCommit(req.getTxnResolution());
        if (txResolutionResponse.getTokenType() != TokenType.NORMAL) {
            // If the txn aborts, then DO NOT hand out a token.
            Token newToken = new Token(sequencerEpoch, txResolutionResponse.getAddress());
            r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(new TokenResponse(
                    txResolutionResponse.getTokenType(),
                    txResolutionResponse.getConflictingKey(),
                    txResolutionResponse.getConflictingStream(),
                    newToken, Collections.emptyMap(), Collections.emptyMap())));
            return;
        }

        // if we get here, this means the transaction can commit.
        // handleAllocation() does the actual allocation of log position(s)
        // and returns the response
        handleAllocation(msg, ctx, r);
    }

    /**
     * this method serves token-requests for transaction-commit entries.
     *
     * <p>it checks if the transaction can commit.
     * - if the transaction must abort,
     * then a 'error token' containing an Address.ABORTED address is returned.
     * - if the transaction may commit,
     * then a normal allocation of log position(s) is pursued.
     *
     * @param req corfu message containing transaction token
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleTxToken(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        final CorfuProtocol.TokenRequest tokenRequest = req.getTokenRequest();

        // in the TK_TX request type, the sequencer is utilized for transaction conflict-resolution.
        // Token allocation is conditioned on commit.
        // First, we check if the transaction can commit.
        CorfuProtocol.TxResolutionResponse txResolutionResponse = txnCanCommit(tokenRequest.getTxnResolution());
        if (txResolutionResponse.getTokenType() != CorfuProtocol.TokenType.TX_NORMAL) {
            // If the txn aborts, then DO NOT hand out a token.
            CorfuProtocol.Token newToken = getToken(sequencerEpoch, txResolutionResponse.getAddress());

            Header responseHeader = API.generateResponseHeader(req.getHeader(),
                    req.getHeader().getIgnoreClusterId(), req.getHeader().getIgnoreEpoch());
            CorfuProtocol.TokenResponse tokenResponse = getTokenResponse(txResolutionResponse.getTokenType(),
                    txResolutionResponse.getConflictingKey(),
                    txResolutionResponse.getConflictingStream(),
                    newToken,
                    UUIDToLongMap.getDefaultInstance(),
                    UUIDToLongMap.getDefaultInstance());
            Response response = getTokenResponse(responseHeader, tokenResponse);
            r.sendResponse(response, ctx);
            return;
        }

        // if we get here, this means the transaction can commit.
        // handleAllocation() does the actual allocation of log position(s)
        // and returns the response
        handleAllocation(req, ctx, r);
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * this method does the actual allocation of log addresses,
     * it also maintains stream-tails, returns a map of stream-tails for backpointers,
     * and maintains a conflict-parameters map.
     *
     * @param msg corfu message containing allocation
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleAllocation(CorfuPayloadMsg<TokenRequest> msg,
                                  ChannelHandlerContext ctx, IServerRouter r) {
        final TokenRequest req = msg.getPayload();

        // extend the tail of the global log by the requested # of tokens
        // currentTail is the first available position in the global log
        long newTail = globalLogTail + req.getNumTokens();

        // for each stream:
        //   1. obtain the last back-pointer for this stream, if exists; -1L otherwise.
        //   2. record the new global tail as back-pointer for this stream.
        //   3. Add the allocated addresses to each stream's address map.
        ImmutableMap.Builder<UUID, Long> backPointerMap = ImmutableMap.builder();
        for (UUID id : req.getStreams()) {

            // step 1. and 2. (comment above)
            streamTailToGlobalTailMap.compute(id, (k, v) -> {
                if (v == null) {
                    backPointerMap.put(k, Address.NON_EXIST);
                    return newTail - 1;
                } else {
                    backPointerMap.put(k, v);
                    return newTail - 1;
                }
            });

            // step 3. add allocated addresses to each stream's address map (to keep track of all updates to this stream)
            streamsAddressMap.compute(id, (streamId, addressMap) -> {
                if (addressMap == null) {
                    addressMap = new StreamAddressSpace(Address.NON_ADDRESS, new Roaring64NavigableMap());
                }

                for (long i = globalLogTail; i < newTail; i++) {
                    addressMap.addAddress(i);
                }
                return addressMap;
            });
        }

        // update the cache of conflict parameters
        if (req.getTxnResolution() != null) {
            req.getTxnResolution()
                    .getWriteConflictParams()
                    .forEach((key, value) -> {
                        // insert an entry with the new timestamp using the
                        // hash code based on the param and the stream id.
                        value.forEach(conflictParam ->
                                cache.put(new ConflictTxStream(key, conflictParam, newTail - 1)));
                    });
        }

        log.trace("token {} backpointers {}", globalLogTail, backPointerMap.build());

        // return the token response with the global tail and the streams backpointers
        Token token = new Token(sequencerEpoch, globalLogTail);
        globalLogTail = newTail;
        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                new TokenResponse(token, backPointerMap.build())));
    }

    /**
     * this method does the actual allocation of log addresses,
     * it also maintains stream-tails, returns a map of stream-tails for backpointers,
     * and maintains a conflict-parameters map.
     *
     * @param req corfu message containing allocation
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleAllocation(Request req,
                                  ChannelHandlerContext ctx, IRequestRouter r) {
        final CorfuProtocol.TokenRequest tokenRequest = req.getTokenRequest();

        // extend the tail of the global log by the requested # of tokens
        // currentTail is the first available position in the global log
        long newTail = globalLogTail + tokenRequest.getNumTokens();

        // for each stream:
        //   1. obtain the last back-pointer for this stream, if exists; -1L otherwise.
        //   2. record the new global tail as back-pointer for this stream.
        //   3. Add the allocated addresses to each stream's address map.
        ImmutableMap.Builder<UUID, Long> backPointerMap = ImmutableMap.builder();
        for (CorfuProtocol.UUID id : tokenRequest.getStreamsList()) {

            // step 1. and 2. (comment above)
            streamTailToGlobalTailMap.compute(getJavaUUID(id), (k, v) -> {
                if (v == null) {
                    backPointerMap.put(k, Address.NON_EXIST);
                } else {
                    backPointerMap.put(k, v);
                }
                return newTail - 1;
            });

            // step 3. add allocated addresses to each stream's address map (to keep track of all updates to this stream)
            streamsAddressMap.compute(getJavaUUID(id), (streamId, addressMap) -> {
                if (addressMap == null) {
                    addressMap = new StreamAddressSpace(Address.NON_ADDRESS, new Roaring64NavigableMap());
                }

                for (long i = globalLogTail; i < newTail; i++) {
                    addressMap.addAddress(i);
                }
                return addressMap;
            });
        }

        // update the cache of conflict parameters
        if (tokenRequest.getTxnResolution() != null) {
            tokenRequest.getTxnResolution()
                    .getWriteConflictParamsSet().getEntriesList()
                    .forEach((uuidToListOfBytesPair) -> {
                        // insert an entry with the new timestamp using the
                        // hash code based on the param and the stream id.
                        uuidToListOfBytesPair.getValueList().forEach(conflictParam ->
                                cache.put(new ConflictTxStream(getJavaUUID(uuidToListOfBytesPair.getKey()),
                                        conflictParam.toByteArray(), newTail - 1)));
                    });
        }

        log.trace("token {} backpointers {}", globalLogTail, backPointerMap.build());

        // return the token response with the global tail and the streams backpointers
        CorfuProtocol.Token newToken = getToken(sequencerEpoch, globalLogTail);
        globalLogTail = newTail;
        Header responseHeader = API.generateResponseHeader(req.getHeader(),
                req.getHeader().getIgnoreClusterId(), req.getHeader().getIgnoreEpoch());
        CorfuProtocol.TokenResponse tokenResponse = getTokenResponse(newToken,
                getProtoUUIDToLongMap(backPointerMap.build()));
        Response response = getTokenResponse(responseHeader,tokenResponse);
        r.sendResponse(response, ctx);
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * This method handles the request of streams addresses.
     *
     * The request of address spaces can be of two types:
     *      - For specific streams (and specific ranges for each stream).
     *      - For all streams (complete range).
     *
     * The response contains the requested streams address maps and the global log tail.
     */
    @ServerHandler(type = CorfuMsgType.STREAMS_ADDRESS_REQUEST)
    private void handleStreamsAddressRequest(CorfuPayloadMsg<StreamsAddressRequest> msg,
                                             ChannelHandlerContext ctx, IServerRouter r) {
        StreamsAddressRequest req = msg.getPayload();
        Map<UUID, StreamAddressSpace> streamsAddressMap;

        switch (req.getReqType()) {
            case StreamsAddressRequest.STREAMS:
                streamsAddressMap = getStreamsAddresses(req.getStreamsRanges());
                break;

            default:
                // Retrieve address space for all streams
                streamsAddressMap = new HashMap<>(this.streamsAddressMap);
                break;
        }

        log.trace("handleStreamsAddressRequest: return address space for streams [{}]",
                streamsAddressMap.keySet());
        r.sendResponse(ctx, msg, CorfuMsgType.STREAMS_ADDRESS_RESPONSE.payloadMsg(
                new StreamsAddressResponse(getGlobalLogTail(), streamsAddressMap)));
    }

    /**
     * This method handles the request of streams addresses.
     *
     * The request of address spaces can be of two types:
     *      - For specific streams (and specific ranges for each stream).
     *      - For all streams (complete range).
     *
     * The response contains the requested streams address maps and the global log tail.
     */
    @RequestHandler(type = CorfuProtocol.MessageType.STREAMS_ADDRESS)
    private void handleStreamsAddressRequest(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        CorfuProtocol.StreamsAddressRequest streamsAddressRequest = req.getStreamsAddressRequest();
        Map<UUID, StreamAddressSpace> streamsAddressMap;

        if (streamsAddressRequest.getReqType() == CorfuProtocol.StreamsAddressRequest.Type.STREAMS) {
            streamsAddressMap = getStreamsAddressesProto(streamsAddressRequest.getStreamsRangesList());
        } else {// Retrieve address space for all streams
            streamsAddressMap = new HashMap<>(this.streamsAddressMap);
        }

        log.trace("handleStreamsAddressRequest: return address space for streams [{}]",
                streamsAddressMap.keySet());

        // Converting Map<UUID, StreamAddressSpace> to proto UUIDToStreamAddressMap
        UUIDToStreamAddressMap.Builder addressMapBuilder = UUIDToStreamAddressMap.newBuilder();
        streamsAddressMap.forEach((uuid, streamAddressSpace) -> {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
            try {
                streamAddressSpace.getAddressMap().serialize(outputStream);
                ByteString addressMap = ByteString.copyFrom(byteArrayOutputStream.toByteArray());
                outputStream.close();

                // Create and add UUIDToStreamAddressPair entries
                addressMapBuilder.addEntries(
                        UUIDToStreamAddressPair.newBuilder()
                                .setKey(getUUID(uuid))
                                .setValue(
                                        CorfuProtocol.StreamAddressSpace.newBuilder()
                                                .setTrimMark(streamAddressSpace.getTrimMark())
                                                .setAddressMap(addressMap)
                                )
                                .build()
                );

            } catch (IOException e) {
                log.error("resetServer: error while serializing roaring64NavigableMap");
            }
        });

        Header responseHeader = API.generateResponseHeader(req.getHeader(),
                req.getHeader().getIgnoreClusterId(), req.getHeader().getIgnoreEpoch());

        Response response = getStreamsAddressResponse(responseHeader,
                getGlobalLogTail(), addressMapBuilder.build());
        r.sendResponse(response, ctx);
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * Return the address space for each stream in the requested ranges.
     *
     * @param addressRanges list of requested a stream and ranges.
     * @return map of stream to address space.
     */
    private Map<UUID, StreamAddressSpace> getStreamsAddresses(List<StreamAddressRange> addressRanges) {
        Map<UUID, StreamAddressSpace> requestedAddressSpaces = new HashMap<>();
        Roaring64NavigableMap addressMap;

        for (StreamAddressRange streamAddressRange : addressRanges) {
            UUID streamId = streamAddressRange.getStreamID();
            // Get all addresses in the requested range
            if (streamsAddressMap.containsKey(streamId)) {
                addressMap = streamsAddressMap.get(streamId).getAddressesInRange(streamAddressRange);
                requestedAddressSpaces.put(streamId,
                        new StreamAddressSpace(streamsAddressMap.get(streamId).getTrimMark(), addressMap));
            } else {
                log.warn("handleStreamsAddressRequest: address space map is not present for stream {}. " +
                        "Verify this is a valid stream.", streamId);
            }
        }

        return requestedAddressSpaces;
    }

    /**
     * Return the address space for each stream in the requested ranges.
     *
     * @param addressRanges list of requested a stream and ranges.
     * @return map of stream to address space.
     */
    private Map<UUID, StreamAddressSpace> getStreamsAddressesProto(List<CorfuProtocol.StreamAddressRange> addressRanges) {
        Map<UUID, StreamAddressSpace> requestedAddressSpaces = new HashMap<>();
        Roaring64NavigableMap addressMap;

        for (CorfuProtocol.StreamAddressRange streamAddressRange : addressRanges) {
            CorfuProtocol.UUID streamId = streamAddressRange.getStreamID();
            // Get all addresses in the requested range
            if (streamsAddressMap.containsKey(getJavaUUID(streamId))) {
                addressMap = getAddressesInRange(streamAddressRange,
                        streamsAddressMap.get(getJavaUUID(streamId)).getAddressMap());
                requestedAddressSpaces.put(getJavaUUID(streamId),
                        new StreamAddressSpace(streamsAddressMap.get(getJavaUUID(streamId)).getTrimMark(),
                                addressMap));
            } else {
                log.warn("handleStreamsAddressRequest: address space map is not present for stream {}. " +
                        "Verify this is a valid stream.", streamId);
            }
        }

        return requestedAddressSpaces;
    }

    /**
     * Get addresses in range (end, start], where start > end.
     * (This method's proto version was copied to this class from {@link StreamAddressSpace})
     *
     * @return Bitmap with addresses in this range.
     */
    public Roaring64NavigableMap getAddressesInRange(CorfuProtocol.StreamAddressRange range,
                                                     Roaring64NavigableMap addressMap) {
        Roaring64NavigableMap addressesInRange = new Roaring64NavigableMap();
        if (range.getStart() > range.getEnd()) {
            addressMap.forEach(address -> {
                // Because our search is referenced to the stream's tail => (end < start]
                if (address > range.getEnd() && address <= range.getStart()) {
                    addressesInRange.add(address);
                }
            });
        }

        log.trace("getAddressesInRange[{}]: address map in range [{}-{}] has a total of {} addresses.",
                Utils.toReadableId(range.getStreamID()), range.getEnd(),
                range.getStart(), addressesInRange.getLongCardinality());

        return addressesInRange;
    }



    /**
     * Sequencer server configuration
     */
    @Builder
    @Getter
    public static class Config {
        private static final int DEFAULT_CACHE_SIZE = 250_000;

        @Default
        private final int cacheSize = DEFAULT_CACHE_SIZE;

        public static Config parse(Map<String, Object> opts) {
            int cacheSize = (int)(opts.containsKey("--sequencer-cache-size") ?
            Integer.parseInt((String)opts.get("--sequencer-cache-size")) : DEFAULT_CACHE_SIZE);
            return Config.builder()
                    .cacheSize(cacheSize)
                    .build();
        }
    }
}
