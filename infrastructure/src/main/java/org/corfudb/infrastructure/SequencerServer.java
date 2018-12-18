package org.corfudb.infrastructure;

import static org.corfudb.protocols.wireprotocol.TokenType.TX_ABORT_NEWSEQ;
import static org.corfudb.protocols.wireprotocol.TokenType.TX_ABORT_SEQ_OVERFLOW;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.SequencerMetrics.SequencerStatus;
import org.corfudb.protocols.wireprotocol.SequencerTailsRecoveryMsg;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
     * Our options.
     */
    private final Map<String, Object> opts;

    /**
     * - {@link SequencerServer::globalLogTail}:
     * global log first available position (initially, 0).
     */
    @Getter
    private final AtomicLong globalLogTail = new AtomicLong(Address
            .getMinAddress());

    private long trimMark = Address.NON_ADDRESS;

    /**
     * - {@link SequencerServer::streamTailToGlobalTailMap}:
     * per streams map to last issued global-log position. used for
     * backpointers.
     */
    private final ConcurrentHashMap<UUID, Long> streamTailToGlobalTailMap = new
            ConcurrentHashMap<>();

    /**
     * TX conflict-resolution information:
     *
     * {@link SequencerServer::conflictToGlobalTailCache}:
     * a cache of recent conflict keys and their latest global-log
     * position.
     *
     * {@link SequencerServer::maxConflictWildcard} :
     * a "wildcard" representing the maximal update timestamp of
     * all the conflict keys which were evicted from the cache
     *
     * * {@link SequencerServer::maxConflictNewSequencer} :
     * represents the max update timestamp of all the conflict keys
     * which were evicted from the cache by the time this server is elected
     * the primary sequencer. This means that any snapshot timestamp below this
     * actual threshold would abort due to NEW_SEQUENCER cause.
     */
    private final Cache<String, Long> conflictToGlobalTailCache;

    private long maxConflictWildcard = Address.NOT_FOUND;

    private long maxConflictNewSequencer = Address.NOT_FOUND;

    /**
     * A map to cache the name of timers to avoid creating timer names on each call.
     */
    private final Map<Byte, String> timerNameCache = new HashMap<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Handler for this server.
     */
    @Getter
    private final CorfuMsgHandler handler =
            CorfuMsgHandler.generateHandler(MethodHandles.lookup(), this);


    @Getter
    @Setter
    private volatile long sequencerEpoch = Layout.INVALID_EPOCH;

    @Override
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        if ((sequencerEpoch != serverContext.getServerEpoch())
                && (!msg.getMsgType().equals(CorfuMsgType.BOOTSTRAP_SEQUENCER))) {
            log.warn("Rejecting msg at sequencer : sequencerStateEpoch:{}, serverEpoch:{}, "
                    + "msg:{}", sequencerEpoch, serverContext.getServerEpoch(), msg);
            return false;
        }
        return true;
    }

    ThreadFactory threadFactory = new ServerThreadFactory("sequencer-",
            new ServerThreadFactory.ExceptionHandler());

    private final ExecutorService executor = Executors.newFixedThreadPool(2, threadFactory);

    @Override
    public ExecutorService getExecutor() {
        return executor;
    }

    /**
     * Returns a new SequencerServer.
     * @param serverContext context object providing parameters and objects
     */
    public SequencerServer(ServerContext serverContext) {
        this.serverContext = serverContext;
        this.opts = serverContext.getServerConfig();

        long initialToken = Utils.parseLong(opts.get("--initial-token"));
        if (Address.nonAddress(initialToken)) {
            globalLogTail.set(0L);
        } else {
            globalLogTail.set(initialToken);
        }

        long cacheSize = 250_000;
        if (opts.get("--sequencer-cache-size") != null) {
            cacheSize = Long.parseLong((String) opts.get("--sequencer-cache-size"));

        }
        conflictToGlobalTailCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .removalListener((String k, Long v, RemovalCause cause) -> {
                    if (!RemovalCause.REPLACED.equals(cause)) {
                         log.trace("Updating maxConflictWildcard. Old value = '{}', new value='{}'"
                                        + " conflictParam = '{}'. Removal cause = '{}'",
                                maxConflictWildcard, v, k, cause);
                        maxConflictWildcard = Math.max(v, maxConflictWildcard);
                    }
                })
                .recordStats()
                .build();

        setUpTimerNameCache();
    }

    /**
     * Initialized the hashmap with the name of timers for different types of requests
     */
    private void setUpTimerNameCache() {
        timerNameCache.put(TokenRequest.TK_QUERY, CorfuComponent.INFRA_SEQUENCER + "query-token");
        timerNameCache.put(TokenRequest.TK_RAW, CorfuComponent.INFRA_SEQUENCER + "raw-token");
        timerNameCache.put(TokenRequest.TK_MULTI_STREAM, CorfuComponent.INFRA_SEQUENCER +
                "multi-stream-token");
        timerNameCache.put(TokenRequest.TK_TX, CorfuComponent.INFRA_SEQUENCER + "tx-token");
    }

    /**
    * Get the conflict hash code for a stream ID and conflict param.
    *
    * @param streamId      The stream ID.
    * @param conflictParam The conflict parameter.
    * @return A conflict hash code.
    */
    private String getConflictHashCode(UUID streamId, byte[] conflictParam) {
        return streamId.toString() + Utils.bytesToHex(conflictParam);
    }

    /**
     * If the request submits a timestamp (a global offset) that is less than one of the
     * global offsets of a streams specified in the request, then abort; otherwise commit.
     *
     * @param txInfo      info provided by corfuRuntime for conflict resolultion:
     *                    - timestamp : the snapshot (global) offset that this TX reads
     *                    - conflictSet: conflict set of the txn.
     *                    if any conflict-param (or stream, if empty) in this set has a later
     *                    timestamp than the snapshot, abort
     * @param conflictKey is a return parameter that signals to the consumer which key was
     *                    responsible for unsuccessful allocation af a token.
     * @return Returns the type of token reponse based on whether the txn commits, or the abort
     *     cause.
     */
    private TokenType txnCanCommit(TxResolutionInfo txInfo, AtomicReference<byte[]> conflictKey) {
        log.trace("Commit-req[{}]", txInfo);
        final Token txSnapshotTimestamp = txInfo.getSnapshotTimestamp();

        // A transaction can start with a timestamp issued from a previous
        // epoch, so we need to reject transactions that have a snapshot
        // timestamp with a different epoch than the this sequencer's epoch.
        if (txSnapshotTimestamp.getEpoch() != sequencerEpoch) {
            log.debug("ABORT[{}] snapshot-ts[{}] current epoch[{}]", txInfo, txSnapshotTimestamp, sequencerEpoch);
            return TokenType.TX_ABORT_NEWSEQ;
        }

        if (txSnapshotTimestamp.getSequence() < trimMark) {
            log.debug("ABORT[{}] snapshot-ts[{}] trimMark-ts[{}]", txInfo, txSnapshotTimestamp, trimMark);
            return TokenType.TX_ABORT_SEQ_TRIM;
        }

        AtomicReference<TokenType> response = new AtomicReference<>(TokenType.NORMAL);

        for (Map.Entry<UUID, Set<byte[]>> entry : txInfo.getConflictSet().entrySet()) {
            if (response.get() != TokenType.NORMAL) {
                break;
            }

            // if conflict-parameters are present, check for conflict based on conflict-parameter
            // updates
            Set<byte[]> conflictParamSet = entry.getValue();
            if (conflictParamSet != null && !conflictParamSet.isEmpty()) {
                // for each key pair, check for conflict if not present, check against the wildcard
                for (byte[] conflictParam : conflictParamSet) {

                    String conflictKeyHash = getConflictHashCode(entry.getKey(),
                            conflictParam);
                    Long v = conflictToGlobalTailCache.getIfPresent(conflictKeyHash);

                    log.trace("Commit-ck[{}] conflict-key[{}](ts={})", txInfo, conflictParam, v);

                    if (v != null && v > txSnapshotTimestamp.getSequence()) {
                        log.debug("ABORT[{}] conflict-key[{}](ts={})", txInfo, conflictParam, v);
                        conflictKey.set(conflictParam);
                        response.set(TokenType.TX_ABORT_CONFLICT);
                        break;
                    }

                    // The maxConflictNewSequencer is modified whenever a server is elected
                    // as the 'new' sequencer, we immediately set its value to the max timestamp
                    // evicted from the cache at that time. If a txSnapshotTimestamp falls
                    // under this threshold we can report that the cause of abort is due to
                    // a NEW_SEQUENCER (not able to hold these in its cache).
                    if (txSnapshotTimestamp.getSequence() < maxConflictNewSequencer) {
                        log.debug("ABORT[{}] snapshot-ts[{}] WILDCARD New Sequencer ts=[{}]",
                                txInfo, txSnapshotTimestamp, maxConflictNewSequencer);
                        response.set(TX_ABORT_NEWSEQ);
                        break;
                    }

                    // If the txSnapshotTimestamp did not fall under the new sequencer threshold
                    // but it does fall under the latest evicted timestamp we report the cause of
                    // abort as SEQUENCER_OVERFLOW
                    if (txSnapshotTimestamp.getSequence() < maxConflictWildcard) {
                        log.debug("ABORT[{}] snapshot-ts[{}] WILDCARD ts=[{}]",
                                txInfo, txSnapshotTimestamp, maxConflictWildcard);
                        response.set(TX_ABORT_SEQ_OVERFLOW);
                        break;
                    }
                }
            } else { // otherwise, check for conflict based on streams updates
                UUID streamId = entry.getKey();
                streamTailToGlobalTailMap.compute(streamId, (k, v) -> {
                    if (v == null) {
                        return null;
                    }
                    if (v > txSnapshotTimestamp.getSequence()) {
                        log.debug("ABORT[{}] conflict-stream[{}](ts={})", txInfo, Utils.toReadableId(streamId), v);
                        response.set(TokenType.TX_ABORT_CONFLICT);
                    }
                    return v;
                });
            }
        }

        return response.get();
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
    private void handleTokenQuery(CorfuPayloadMsg<TokenRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        TokenRequest req = msg.getPayload();
        List<UUID> streams = req.getStreams();
        List<Long> streamTails;
        Token token;

        try {
            lock.writeLock().lock();

            if (req.getStreams().isEmpty()) {
                // Global tail query
                token = new Token(sequencerEpoch, globalLogTail.get() - 1);
                streamTails = Collections.emptyList();
            } else if (req.getStreams().size() == 1) {
                // single stream query
                token = new Token(
                        sequencerEpoch,
                        streamTailToGlobalTailMap.getOrDefault(streams.get(0), Address.NON_EXIST)
                );
                streamTails = Collections.emptyList();
            } else {
                // multiple stream query, the token is populated with the global tail and the tail queries are stored in
                // streamTails
                token = new Token(sequencerEpoch, globalLogTail.get() - 1);
                streamTails = new ArrayList<>(streams.size());
                for (UUID stream : streams) {
                    streamTails.add(streamTailToGlobalTailMap.getOrDefault(stream, Address.NON_EXIST));
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

        TokenResponse payload = new TokenResponse(
                TokenType.NORMAL, TokenResponse.NO_CONFLICT_KEY, token, Collections.emptyMap(), streamTails
        );

        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(payload));
    }


    @ServerHandler(type = CorfuMsgType.SEQUENCER_TRIM_REQ)
    public void trimCache(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx, IServerRouter r) {
        try {
            lock.writeLock().lock();
            log.info("trimCache: Starting cache eviction");
            if (trimMark < msg.getPayload()) {
                // Advance the trim mark, if the new trim request has a higher trim mark.
                trimMark = msg.getPayload();
            }

            long entries = 0;
            for (Map.Entry<String, Long> entry : conflictToGlobalTailCache.asMap().entrySet()) {
                if (entry.getValue() < trimMark) {
                    conflictToGlobalTailCache.invalidate(entry.getKey());
                    entries++;
                }
            }
            log.info("trimCache: Evicted {} entries", entries);
        } finally {
            lock.writeLock().unlock();
        }

        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    /**
     * Service an incoming request to reset the sequencer.
     */
    @ServerHandler(type = CorfuMsgType.BOOTSTRAP_SEQUENCER)
    public void resetServer(CorfuPayloadMsg<SequencerTailsRecoveryMsg> msg, ChannelHandlerContext ctx, IServerRouter r) {
        long initialToken = msg.getPayload().getGlobalTail();
        final Map<UUID, Long> streamTails = msg.getPayload().getStreamTails();
        final long bootstrapMsgEpoch = msg.getPayload().getSequencerEpoch();

        // Boolean flag to denote whether this bootstrap message is just updating an existing
        // primary sequencer with the new epoch (if set to true) or bootstrapping a currently
        // NOT_READY sequencer.
        final boolean bootstrapWithoutTailsUpdate = msg.getPayload()
                .getBootstrapWithoutTailsUpdate();

        boolean ack = true;
        try {
            lock.writeLock().lock();

            // If sequencerEpoch is -1 (startup) OR bootstrapMsgEpoch is not the consecutive epoch of
            // the sequencerEpoch then the sequencer should not accept bootstrapWithoutTailsUpdate
            // bootstrap messages.
            if (bootstrapWithoutTailsUpdate
                    && (sequencerEpoch == Layout.INVALID_EPOCH
                    || bootstrapMsgEpoch != sequencerEpoch + 1)) {
                log.warn("Cannot update existing sequencer. Require full bootstrap. "
                        + "SequencerEpoch : {}, MsgEpoch : {}", sequencerEpoch, bootstrapMsgEpoch);
                ack = false;
            } else if (serverContext.getSequencerEpoch() >= bootstrapMsgEpoch) {
                // Stale bootstrap request should be discarded.
                log.info("Sequencer already bootstrapped at epoch {}. "
                        + "Discarding bootstrap request with epoch {}", sequencerEpoch, bootstrapMsgEpoch);
                ack = false;
            }

            // If the sequencer is reset, then we can't know when was
            // the latest update to any stream or conflict parameter.
            // hence, we will accept any bootstrap message with a higher epoch and forget any existing
            // token count or stream tails.
            //
            // Note, this is correct, but conservative (may lead to false abort).
            // It is necessary because we reset the sequencer.
            if (!bootstrapWithoutTailsUpdate) {
                // Evict all entries from the cache. This eviction triggers the callback modifying the maxConflictWildcard.
                conflictToGlobalTailCache.cleanUp();

                globalLogTail.set(initialToken);
                maxConflictWildcard = initialToken - 1;
                maxConflictNewSequencer = maxConflictWildcard;

                // Clear the existing map as it could have been populated by an earlier reset.
                streamTailToGlobalTailMap.clear();
                streamTailToGlobalTailMap.putAll(streamTails);
            }

            // Mark the sequencer as ready after the tails have been populated.
            sequencerEpoch = bootstrapMsgEpoch;
            serverContext.setSequencerEpoch(bootstrapMsgEpoch);

            log.info("Sequencer reset with token = {}, size {} streamTailToGlobalTailMap = {}, sequencerEpoch = {}",
                    globalLogTail.get(), streamTailToGlobalTailMap.size(), streamTailToGlobalTailMap, sequencerEpoch);
        } finally {
            lock.writeLock().unlock();
        }

        if (ack) {
            r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        } else {
            r.sendResponse(ctx, msg, CorfuMsgType.NACK.msg());
        }
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
    public void tokenRequest(CorfuPayloadMsg<TokenRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        TokenRequest req = msg.getPayload();
        Timer timer = getTimer(req.getReqType());

        // dispatch request handler according to request type while collecting the timer metrics
        try (Context context = MetricsUtils.getConditionalContext(timer)) {
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
        String timerName = timerNameCache.getOrDefault(reqType, CorfuComponent.INFRA_SEQUENCER + "unknown");
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

        Token token;
        try {
            lock.writeLock().lock();
            token = new Token(sequencerEpoch, globalLogTail.getAndAdd(req.getNumTokens()));
        } finally {
            lock.writeLock().unlock();
        }

        TokenResponse tokenResponse = new TokenResponse(
                TokenType.NORMAL,
                TokenResponse.NO_CONFLICT_KEY,
                token,
                Collections.emptyMap(),
                Collections.emptyList()
        );
        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(tokenResponse));
    }

    /**
     * this method serves token-requests for transaction-commit entries.
     *
     * <p>it checks if the transaction can commit.
     * - if the transction must abort,
     * then a 'error token' containing an Address.ABORTED address is returned.
     * - if the transaction may commit,
     * then a normal allocation of log position(s) is pursued.
     *
     * @param msg corfu message containing transaction token
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleTxToken(CorfuPayloadMsg<TokenRequest> msg,
                               ChannelHandlerContext ctx, IServerRouter r) {
        final TokenRequest req = msg.getPayload();

        // Since Java does not allow an easy way for a function to return multiple values, this
        // variable is passed to the consumer that will use it to indicate to us if/what key was
        // responsible for an aborted transaction.
        AtomicReference<byte[]> conflictKey = new AtomicReference(TokenResponse.NO_CONFLICT_KEY);

        Token token = null;
        TokenType tokenType;
        boolean aborted = false;

        try {
            lock.writeLock().lock();
            // in the TK_TX request type, the sequencer is utilized for transaction conflict-resolution.
            // Token allocation is conditioned on commit.
            // First, we check if the transaction can commit.
            tokenType = txnCanCommit(req.getTxnResolution(), conflictKey);
            if (tokenType != TokenType.NORMAL) {
                // If the txn aborts, then DO NOT hand out a token.
                token = new Token(sequencerEpoch, Address.ABORTED);
                aborted = true;
            }
        } finally {
            lock.writeLock().unlock();
        }

        if (aborted) {
            TokenResponse payload = new TokenResponse(
                    tokenType,
                    conflictKey.get(),
                    token,
                    Collections.emptyMap(),
                    Collections.emptyList()
            );
            r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(payload));
            return;
        }

        // if we get here, this means the transaction can commit.
        // handleAllocation() does the actual allocation of log position(s)
        // and returns the reponse
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
    private void handleAllocation(CorfuPayloadMsg<TokenRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        final TokenRequest req = msg.getPayload();

        Token token;
        ImmutableMap.Builder<UUID, Long> backPointerMap = ImmutableMap.builder();
        try {
            lock.writeLock().lock();

            // extend the tail of the global log by the requested # of tokens
            // currentTail is the first available position in the global log
            long currentTail = globalLogTail.getAndAdd(req.getNumTokens());
            long newTail = currentTail + req.getNumTokens();

            // for each streams:
            //   1. obtain the last back-pointer for this streams, if exists; -1L otherwise.
            //   2. record the new global tail as back-pointer for this streams.
            //   3. extend the tail by the requested # tokens.
            for (UUID id : req.getStreams()) {

                // step 1. and 2. (comment above)
                streamTailToGlobalTailMap.compute(id, (k, v) -> {
                    long address = v == null ? Address.NON_EXIST: v;
                    backPointerMap.put(k, address);
                    return newTail - 1;
                });
            }

            // update the cache of conflict parameters
            if (req.getTxnResolution() != null) {
                // for each entry
                // and for each conflict param
                Map<UUID, Set<byte[]>> conflictParams = req.getTxnResolution().getWriteConflictParams();

                for (UUID key : conflictParams.keySet()) {
                    // insert an entry with the new timestamp using the hash code based on the param and the stream id.
                    conflictParams.get(key).forEach(conflictParam ->
                            conflictToGlobalTailCache.put(getConflictHashCode(key, conflictParam), newTail - 1)
                    );
                }
            }

            log.trace("token {} backpointers {}", currentTail, backPointerMap.build());
            // return the token response with the new global tail and the streams backpointers
            token = new Token(sequencerEpoch, currentTail);
        } finally {
          lock.writeLock().unlock();
        }

        TokenResponse payload = new TokenResponse(
                TokenType.NORMAL,
                TokenResponse.NO_CONFLICT_KEY,
                token,
                backPointerMap.build(),
                Collections.emptyList()
        );
        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(payload));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdownNow();
    }

    @VisibleForTesting
    public Cache<String, Long> getConflictToGlobalTailCache() {
        return conflictToGlobalTailCache;
    }
}
