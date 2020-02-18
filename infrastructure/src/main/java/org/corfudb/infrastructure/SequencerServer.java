package org.corfudb.infrastructure;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelHandlerContext;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

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

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This server implements the sequencer functionality of Corfu.
 *
 * <p>It currently supports a single operation, which is a incoming request:
 *
 * <p>TOKEN_REQ - Request the next address.
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
     * A map to cache the name of timers to avoid creating timer names on each call.
     */
    private final Map<Byte, String> timerNameCache = new HashMap<>();

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
        setUpTimerNameCache();
    }

    @Override
    protected void processRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        executor.submit(() -> getHandler().handle(msg, ctx, r));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
    }

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

    /**
     * Initialized the HashMap with the name of timers for different types of requests
     */
    private void setUpTimerNameCache() {
        timerNameCache.put(TokenRequest.TK_QUERY, CorfuComponent.INFRA_SEQUENCER + "query-token");
        timerNameCache.put(TokenRequest.TK_RAW, CorfuComponent.INFRA_SEQUENCER + "raw-token");
        timerNameCache.put(TokenRequest.TK_MULTI_STREAM, CorfuComponent.INFRA_SEQUENCER + "multi-stream-token");
        timerNameCache.put(TokenRequest.TK_TX, CorfuComponent.INFRA_SEQUENCER + "tx-token");
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

    /**
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
