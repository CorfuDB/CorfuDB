package org.corfudb.infrastructure;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.BackpointerResponse;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.SequencerTailsRecoveryMsg;
import org.corfudb.protocols.wireprotocol.TokenRequest;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TokenType;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.Utils;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

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
    private final AtomicLong globalLogTail = new AtomicLong(Address.getMinAddress());

    private volatile long trimMark = Address.NON_ADDRESS;

    /**
     * - {@link SequencerServer::streamTailToGlobalTailMap}:
     * per streams map to last issued global-log position. used for
     * backpointers.
     */
    private final MutableObjectLongMap<UUID> streamTailToGlobalTailMap =
            new ObjectLongHashMap<>();

    /**
     * TX conflict-resolution information:
     *
     * {@link SequencerServer::conflictToGlobalTailCache}:
     * a cache of recent conflict keys and their latest global-log
     * position.
     *
     * {@link SequencerServer::maxConflictWildcard} :
     * a "wildcard" representing the maximal update timestamp of
     * all the confict keys which were evicted from the cache
     */
    private long maxConflictWildcard = Address.NOT_FOUND;

    private final Cache<String, Long> conflictToGlobalTailCache;

    /**
     * Handler for this server.
     */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .generateHandlers(MethodHandles.lookup(), this);

    private static final String metricsPrefix = "corfu.server.sequencer.";
    private static Counter counterTokenSum;
    private static Counter counterToken0;

    private final StampedLock lock = new StampedLock();

    @Getter
    @Setter
    private volatile long readyStateEpoch = -1;

    @Override
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        if ((readyStateEpoch != serverContext.getServerEpoch())
                && (!msg.getMsgType().equals(CorfuMsgType.BOOTSTRAP_SEQUENCER))) {
            log.warn("Rejecting msg at sequencer : sequencerStateEpoch:{}, serverEpoch:{}, "
                    + "msg:{}", readyStateEpoch, serverContext.getServerEpoch(), msg);
            return false;
        }
        return true;
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

        MetricRegistry metrics = serverContext.getMetrics();
        counterTokenSum = metrics.counter(metricsPrefix + "token-sum");
        counterToken0 = metrics.counter(metricsPrefix + "token-query");

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
    }

    /**
    * Get the conflict hash code for a stream ID and conflict param.
    *
    * @param streamId      The stream ID.
    * @param conflictParam The conflict parameter.
    * @return A conflict hash code.
    */
    public String getConflictHashCode(UUID streamId, byte[] conflictParam) {
        return streamId.toString() + Utils.bytesToHex(conflictParam);
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
    public void handleTokenQuery(CorfuPayloadMsg<TokenRequest> msg,
                                 ChannelHandlerContext ctx, IServerRouter r) {
        final TokenRequest req = msg.getPayload();
        if (req.getStreams().isEmpty()) {
            r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                    new TokenResponse(globalLogTail.get() - 1,
                    r.getServerEpoch(),
                    Collections.emptyMap())));
        } else {
            final int numStreams = req.getStreams().size();
            // sanity backward-compatibility assertion; TODO: remove
            if (numStreams > 1) {
                log.error("TOKEN-QUERY[{}]", req.getStreams());
            }

            long maxStreamGlobalTail = Address.NON_EXIST;

            // see if this query is for a specific stream-tail
            if (numStreams == 1) {
                UUID streamId = req.getStreams().get(0);

                long ts = lock.tryOptimisticRead();
                if (ts != 0) {
                    maxStreamGlobalTail =
                            streamTailToGlobalTailMap.getIfAbsent(streamId, Address.NON_EXIST);
                }
                if (!lock.validate(ts)) {
                    // lock.validate() calls Unsafe.loadFence(). According to the docs, loadFence:
                    // " Ensures lack of reordering of loads before the fence
                    //   with loads or stores after the fence. "
                    // This means, in the case of of any writer acquiring a lock before the call
                    // to validate, that the read below will reflect -either- the value before
                    // the write lock was acquired or some subsequent future value, which is
                    // the only requirement we need to meet for linearization.
                    maxStreamGlobalTail = streamTailToGlobalTailMap
                            .getIfAbsent(streamId, Address.NON_EXIST);
                }
            }

            r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                    new TokenResponse(maxStreamGlobalTail,
                            r.getServerEpoch(),
                            Collections.emptyMap())));
        }
    }

    @ServerHandler(type = CorfuMsgType.SEQUENCER_TRIM_REQ, opTimer = metricsPrefix + "trimCache")
    public void trimCache(CorfuPayloadMsg<Long> msg,
                                       ChannelHandlerContext ctx, IServerRouter r,
                                       boolean isMetricsEnabled) {
        long ts = 0;
        try {
            ts = lock.writeLock();
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
            lock.unlock(ts);
        }
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    /**
     * Service an incoming request to reset the sequencer.
     */
    @ServerHandler(type = CorfuMsgType.BOOTSTRAP_SEQUENCER, opTimer = metricsPrefix + "reset")
    public void resetServer(CorfuPayloadMsg<SequencerTailsRecoveryMsg> msg,
                                         ChannelHandlerContext ctx, IServerRouter r,
                                         boolean isMetricsEnabled) {
        long ts = lock.writeLock();
        try {
            long initialToken = msg.getPayload().getGlobalTail();
            final Map<UUID, Long> streamTails = msg.getPayload().getStreamTails();
            final long readyEpoch = msg.getPayload().getReadyStateEpoch();

            // Stale bootstrap request should be discarded.
            if (readyStateEpoch > readyEpoch) {
                log.info("Sequencer already bootstrapped at epoch {}. "
                        + "Discarding bootstrap request with epoch {}", readyStateEpoch, readyEpoch);
                r.sendResponse(ctx, msg, CorfuMsgType.NACK.msg());
                return;
            }

            //
            // if the sequencer is reset, then we can't know when was
            // the latest update to any stream or conflict parameter.
            // hence, we want to conservatively abort any transaction with snapshot time
            // preceding the reset-time of this sequencer.
            //
            // Therefore, we remember the new start tail.
            // We empty the cache of conflict parameters.
            // We set the wildcard to the new start tail.
            //
            // Note, this is correct, but conservative (may lead to false abort).
            // It is necessary because we reset the sequencer.
            //
            if (initialToken > globalLogTail.get()) {
                globalLogTail.set(initialToken);
                maxConflictWildcard = initialToken - 1;
                conflictToGlobalTailCache.invalidateAll();

                // Clear the existing map as it could have been populated by an earlier reset.
                streamTailToGlobalTailMap.clear();
                streamTails.entrySet()
                        .forEach(e -> streamTailToGlobalTailMap.put(e.getKey(), e.getValue()));
            }

            // Mark the sequencer as ready after the tails have been populated.
            readyStateEpoch = readyEpoch;

            log.info("Sequencer reset with token = {}, streamTailToGlobalTailMap = {},"
                            + " readyStateEpoch = {}",
                    initialToken, streamTailToGlobalTailMap, readyStateEpoch);
        } finally {
            lock.unlock(ts);
        }
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    /**
     * Service an incoming token request.
     */
    @ServerHandler(type = CorfuMsgType.TOKEN_REQ, opTimer = metricsPrefix + "token-req")
    public void tokenRequest(CorfuPayloadMsg<TokenRequest> msg,
                                          ChannelHandlerContext ctx, IServerRouter r,
                                          boolean isMetricsEnabled) {
        TokenRequest req = msg.getPayload();

        // metrics collection
        if (req.getReqType() == TokenRequest.TK_QUERY) {
            MetricsUtils.incConditionalCounter(isMetricsEnabled, counterToken0, 1);
        } else {
            MetricsUtils.incConditionalCounter(isMetricsEnabled, counterTokenSum, req
                    .getNumTokens());
        }

        long ts = 0;
        CorfuMsg response;
        // dispatch request handler according to request type
        switch (req.getReqType()) {
            case TokenRequest.TK_QUERY:
                handleTokenQuery(msg, ctx, r);
            break;
            case TokenRequest.TK_RAW:
                handleRawToken(msg, ctx, r);
            break;
            case TokenRequest.TK_TX:
                response = handleTxToken(msg, ctx, r);
                r.sendResponse(ctx, msg, response);
            break;
            default:
                try {
                ts = lock.writeLock();
                response = handleAllocationUnsafe(msg, ctx, r);
                r.sendResponse(ctx, msg, response);
                break;
                } finally {
                    lock.unlock(ts);
                }
        }
    }

    /**
     * this method serves log-tokens for a raw log implementation.
     * it simply extends the global log tail and returns the global-log token
     *
     * @param msg corfu message containing raw token
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleRawToken(CorfuPayloadMsg<TokenRequest> msg,
                                ChannelHandlerContext ctx, IServerRouter r) {
        final long serverEpoch = r.getServerEpoch();
        final TokenRequest req = msg.getPayload();

        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(new TokenResponse(
                globalLogTail.getAndAdd(req.getNumTokens()), serverEpoch, Collections.emptyMap())));

    }

    private CorfuMsg verifyCommitUnsafe(final long serverEpoch,
                                        final TxResolutionInfo txInfo,
                                        final long txSnapshotTimestamp) {
        // Iterate over all of the streams in the conflict set
        for (Map.Entry<UUID, Set<byte[]>> entry : txInfo.getConflictSet().entrySet()) {
            final Set<byte[]> conflictParamSet = entry.getValue();
            final UUID streamId = entry.getKey();

            // if conflict-parameters are present, check for conflict
            // based on conflict-parameter updates
            if (conflictParamSet != null && !conflictParamSet.isEmpty()) {
                // for each key pair, check for conflict;
                // if not present, check against the wildcard
                for (byte[] conflictParam : conflictParamSet) {
                    final String conflictKeyHash = getConflictHashCode(streamId, conflictParam);
                    final Long conflictTail = conflictToGlobalTailCache
                            .getIfPresent(conflictKeyHash);
                    final Long validatedAddress =
                            txInfo.getValidatedStreams().get(streamId);

                    if (txSnapshotTimestamp < maxConflictWildcard) {
                        log.debug("ABORT[{}] snapshot-ts[{}] WILDCARD ts=[{}]",
                                txInfo, txSnapshotTimestamp, maxConflictWildcard);
                        return CorfuMsgType.TOKEN_RES.payloadMsg(
                                new TokenResponse(TokenType.TX_ABORT_SEQ_OVERFLOW,
                                        conflictParam, streamId,
                                        Address.ABORTED, serverEpoch));
                    }

                    if (conflictTail != null
                            && conflictTail > txSnapshotTimestamp) {
                        // If we don't have a validation, or if the validation was
                        // for an address before the current conflict tail, we fail.
                        if (validatedAddress == null
                                || validatedAddress < conflictTail) {
                            log.debug("handleTxToken: ABORT[{}] {}conflict-key[{}]({}ts={})",
                                    txInfo,
                                    validatedAddress == null ? "" :
                                            "validation failed (" + validatedAddress + ") ",
                                    conflictParam,
                                    conflictTail);
                            return
                                    CorfuMsgType.TOKEN_RES.payloadMsg(
                                            new TokenResponse(TokenType.TX_ABORT_CONFLICT_KEY,
                                                    conflictParam, streamId,
                                                    conflictTail, serverEpoch));
                        } else {
                            // Otherwise, the client has certified that it has manually checked
                            // that there are no true conflicts, so we continue and issue
                            // the token.
                            log.warn("handleTxToken: validated stream {} {} overrides {}",
                                    Utils.toReadableId(streamId),
                                    validatedAddress,
                                    conflictTail);
                        }
                    }
                    log.trace("handleTxToken: OK[{}] conflict-key[{}](ts={})", txInfo,
                            conflictParam, conflictTail);
                }
            } else { // otherwise, check for conflict based on streams updates
                final Long streamTail = streamTailToGlobalTailMap.get(streamId);
                if (streamTail > txSnapshotTimestamp) {
                    log.debug("handleTxToken: ABORT[{}] conflict-stream[{}](ts={})",
                            txInfo, Utils.toReadableId(streamId), streamTail);
                    return
                            CorfuMsgType.TOKEN_RES.payloadMsg(
                                    new TokenResponse(TokenType.TX_ABORT_CONFLICT_STREAM,
                                            TokenResponse.NO_CONFLICT_KEY,
                                            streamId, streamTail, serverEpoch));
                }
                log.trace("handleTxToken: OK[{}] conflict-stream[{}](ts={})", txInfo,
                        Utils.toReadableId(streamId), streamTail);
            }
        }
        return null;
    }

    /**
     * This method serves token-requests for transaction-commit entries.
     *
     * <p>It checks if the transaction can commit.
     * - if the transaction must abort,
     * then an abort token type is returned, with the address which caused the abort
     * as the token, as well as the conflict key, if present.
     * - if the transaction may commit,
     * then a normal allocation of log position(s) is pursued.
     *
     * @param msg corfu message containing transaction token
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private CorfuMsg handleTxToken(final CorfuPayloadMsg<TokenRequest> msg,
                               final ChannelHandlerContext ctx, final IServerRouter r) {
        final long serverEpoch = r.getServerEpoch();
        final TokenRequest req = msg.getPayload();
        final TxResolutionInfo txInfo = req.getTxnResolution();
        final long txSnapshotTimestamp = txInfo.getSnapshotTimestamp();

        // If the tx has no snapshot (conflict-free) then issue the tokens.
        if (txSnapshotTimestamp == Address.NO_SNAPSHOT) {
            log.debug("handleTxToken[{}]: Conflict-free TX");
            long ts = lock.writeLock();
            try {
                return handleAllocationUnsafe(msg, ctx, r);
            } finally {
                lock.unlock(ts);
            }
        }

        if (txSnapshotTimestamp < trimMark) {
            log.debug("ABORT[{}] snapshot-ts[{}] trimMark-ts[{}]", txInfo,
                    txSnapshotTimestamp, trimMark);
            return CorfuMsgType.TOKEN_RES.payloadMsg(
                            new TokenResponse(TokenType.TX_ABORT_SEQ_TRIM, trimMark, serverEpoch));
        }

        CorfuMsg abortMsg;
        long ts = lock.tryOptimisticRead();
        try {
            abortMsg = verifyCommitUnsafe(serverEpoch, txInfo, txSnapshotTimestamp);
        } catch (Exception e) {
            // An exception may have been thrown due to inconsistent state
            // during an optimistic read (likely a CME). Catch it, and retry.
            abortMsg = null;
        }
        if (abortMsg != null) {
            // We don't validate here - since sequence numbers monotonically increase,
            // an aborted optimistic read will still abort after validation.
            return abortMsg;
        }
        ts = lock.tryConvertToWriteLock(ts);
        if (ts != 0) {
            return handleAllocationUnsafe(msg, ctx, r);
        } else {
            try {
                // Someone else must have grabbed the lock in the meantime.
                // Get a write-lock, revalidate and allocate if safe.
                ts = lock.writeLock();
                abortMsg = verifyCommitUnsafe(serverEpoch, txInfo, txSnapshotTimestamp);
                if (abortMsg != null) {
                    return abortMsg;
                }
                return handleAllocationUnsafe(msg, ctx, r);
            } finally {
                lock.unlock(ts);
            }
        }
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
    private CorfuMsg handleAllocationUnsafe(CorfuPayloadMsg<TokenRequest> msg,
                                            ChannelHandlerContext ctx, IServerRouter r) {
        final long serverEpoch = r.getServerEpoch();
        final TokenRequest req = msg.getPayload();
        final long numTokens = req.getNumTokens();

        // extend the tail of the global log by the requested # of tokens
        // currentTail is the first available position in the global log
        long currentTail = globalLogTail.getAndAdd(numTokens);

        // This is a Long to avoid paying the cost of boxing multiple times
        final long newTail = currentTail + numTokens - 1;

        final List<UUID> streams = req.getStreams();
        // for each streams:
        //   1. obtain the last back-pointer for this streams, if exists; -1L otherwise.
        //   2. record the new global tail as back-pointer for this streams.
        //   3. extend the tail by the requested # tokens.
        BackpointerResponse backPointerMap =
                new BackpointerResponse(streams.size());

        streams
            .forEach(id -> {
                streamTailToGlobalTailMap.updateValue(
                                id,
                                Address.NON_EXIST,
                                v -> {
                                    backPointerMap.add(id, v);
                                    return newTail;
                                });

            });

        // update the cache of conflict parameters
        if (req.getTxnResolution() != null) {
            req.getTxnResolution().getWriteConflictParams()
                    .entrySet()
                    .stream()
                    .flatMap(e ->
                            e.getValue().stream().map(conflictParam ->
                                    getConflictHashCode(e.getKey(), conflictParam)))
                    .forEach(code -> conflictToGlobalTailCache.put(code, newTail));
        }

        log.trace("token {} backpointers {}", currentTail, backPointerMap);
        // return the token response with the new global tail
        // and the streams backpointers
        return CorfuMsgType.TOKEN_RES.payloadMsg(new TokenResponse(
                currentTail, serverEpoch, backPointerMap));
    }

    @VisibleForTesting
    public Cache<String, Long> getConflictToGlobalTailCache() {
        return conflictToGlobalTailCache;
    }
}
