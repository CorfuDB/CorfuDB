package org.corfudb.infrastructure;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.Utils;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.corfudb.util.MetricsUtils.addCacheGauges;

/**
 * This server implements the sequencer functionality of Corfu.
 * <p>
 * It currently supports a single operation, which is a incoming request:
 * <p>
 * TOKEN_REQ - Request the next address.
 *
 * <p>
 * The sequencer server maintains the current tail of the log, the current
 * tail of every stream, and a cache of timestamps of updates on recent
 * conflict-parameters.
 * <p>
 * A token request can be of several sub-types, which are defined in
 * {@link TokenRequest}:
 * <p>
 * {@link TokenRequest::TK_QUERY} - used for only querying the current tail
 * of the log and/or the tails of specific streams
 *
 * {@link TokenRequest::TK_RAW} - reserved for getting a "raw" token in the
 * global log
 *
 * {@link TokenRequest::TK_MULTI_STREAM} - used for logging across one or
 * more streams
 *
 * {@link TokenRequest::TK_TX} - used for reserving an address for transaction
 * commit.
 *
 * The transaction commit is the most sophisticated functaionality of the
 * sequencer. The sequencer reserves an address for the transaction
 * only on if it knows that it can commit.
 *
 * The TK_TX request contains a conflict-set and a write-set. The sequencer
 * checks the conflict-set against the stream-tails and against the
 * conflict-parameters timestamp cache it maintains. If the transaction
 * commits, the sequencer updates the tails of all the streams and the cache
 * of conflict parameters.
 *
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class SequencerServer extends AbstractServer {

    /**
     * key-name for storing {@link SequencerServer} state in {@link ServerContext::getDataStore()}.
     */
    private static final String PREFIX_SEQUENCER = "SEQUENCER";

    /**
     * Inherit from CorfuServer a server context
     */
    private final ServerContext serverContext;

    /**
     * Our options
     */
    private final Map<String, Object> opts;

    private static final String KEY_GLOBAL_LOG_TAIL = "GLOBAL_LOG_TAIL";
    private static final String KEY_STREAM_TAIL_MAP = "STREAM_TAIL_MAP";
    private static final String KEY_STREAM_TAIL_TO_GLOBAL_TAIL_MAP = "STREAM_TAIL_TO_GLOBAL_TAIL_MAP";

    /**  - {@link SequencerServer::globalLogTail}:
     *      global log first available position (initially, 0). */
    @Getter
    private final AtomicLong globalLogTail = new AtomicLong(Address.minAddress());

    /** remember start point, if sequencer is started as failover sequencer */
    private final AtomicLong globalLogStart = new AtomicLong(Address.minAddress());

    /**  - {@link SequencerServer::streamTailMap}:
     *      per-streamfirst available position (initially, null). */
    private final ConcurrentHashMap<UUID, Long> streamTailMap = new ConcurrentHashMap<>();

    /**  - {@link SequencerServer::streamTailToGlobalTailMap}:
     *      per streams map to last issued global-log position. used for
     *      backpointers. */
    private final ConcurrentHashMap<UUID, Long> streamTailToGlobalTailMap = new ConcurrentHashMap<>();

    /**  TX conflict-resolution information:
     *
     * {@link SequencerServer::conflictToGlobalTailCache}:
     *      a cache of recent conflict keys and their latest global-log
     *      position.
     *
     * {@link SequencerServer::maxConflictWildcard} :
     *      a "wildcard" representing the maximal update timestamp of
     *      all the confict keys which were evicted from the cache
     */
    private long maxConflictWildcard = Address.NOT_FOUND;
    private final long maxConflictCacheSize = 1_000_000;
    private final Cache<Integer, Long>
            conflictToGlobalTailCache = Caffeine.newBuilder()
            .maximumSize(maxConflictCacheSize)
            .removalListener((Integer K, Long V, RemovalCause cause) -> {
                if (!RemovalCause.REPLACED.equals(cause)) {
                    log.trace("Updating maxConflictWildcard. Old value = '{}', new value = '{}', conflictParam = '{}'. Removal cause = '{}'",
                            maxConflictWildcard, V, K, cause);
                    maxConflictWildcard = Math.max(V, maxConflictWildcard);
                }
            })
            .recordStats()
            .build();

    /** flag indicating whether this sequencer is the bootstrap
     * sequencer for the log, or not.
     */
    private boolean isFailoverSequencer = false;

    /** Handler for this server */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .generateHandlers(MethodHandles.lookup(), this);

    private static final String metricsPrefix = "corfu.server.sequencer.";
    static private Counter counterTokenSum;
    static private Counter counterToken0;

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
        addCacheGauges(metrics, metricsPrefix + "conflict.cache.", conflictToGlobalTailCache);
    }

    /** Get the conflict hash code for a stream ID and conflict param.
     *
     * @param streamID          The stream ID.
     * @param conflictParam     The conflict parameter.
     * @return                  A conflict hash code.
     */
    public int getConflictHashCode(UUID streamID, int conflictParam) {
            return Objects.hash(streamID, conflictParam);
    }

    /**
     * If the request submits a timestamp (a global offset) that is less than one of the
     * global offsets of a streams specified in the request, then abort; otherwise commit.
     *
     * @param txInfo info provided by corfuRuntime for conflict resolultion:
     *              - timestamp : the snapshot (global) offset that this TX reads
     *              - conflictSet: conflict set of the txn.
     *                if any conflict-param (or stream, if empty) in this set has a later timestamp than the snapshot, abort
     *
     * @return      Returns the type of token reponse based on whether the txn commits, or the abort cause.
     */
    public TokenType txnCanCommit(TxResolutionInfo txInfo) {
        log.trace("Commit-req[{}]", txInfo);
        final long txSnapshotTimestamp = txInfo.getSnapshotTimestamp();

        if (txSnapshotTimestamp < globalLogStart.get()-1) {
            log.debug("ABORT[{}] snapshot-ts[{}] failover-ts[{}]",
                    txSnapshotTimestamp, globalLogStart.get());
            return TokenType.TX_ABORT_NEWSEQ;
        }

        AtomicReference<TokenType> response = new AtomicReference<>(TokenType.NORMAL);

        for (Map.Entry<UUID, Set<Integer>> entry : txInfo.getConflictSet().entrySet()) {
            if (response.get() != TokenType.NORMAL)
                break;

            // if conflict-parameters are present, check for conflict based on conflict-parameter updates
            Set<Integer> conflictParamSet = entry.getValue();
            if (conflictParamSet != null && conflictParamSet.size() > 0) {
                // for each key pair, check for conflict;
                // if not present, check against the wildcard
                conflictParamSet.forEach(conflictParam -> {
                    int conflictKeyHash = getConflictHashCode(entry.getKey(),
                            conflictParam);
                    Long v = conflictToGlobalTailCache.getIfPresent(conflictKeyHash);

                    log.trace("Commit-ck[{}] conflict-key[{}](ts={})", txInfo, conflictParam, v);

                    if (v != null && v > txSnapshotTimestamp ) {
                        log.debug("ABORT[{}] conflict-key[{}](ts={})", txInfo, conflictParam, v);
                        response.set(TokenType.TX_ABORT_CONFLICT);
                    }

                    if (v == null && maxConflictWildcard > txSnapshotTimestamp ) {
                        log.warn("ABORT[{}] conflict-key[{}](WILDCARD ts={})", txInfo, conflictParam,
                                maxConflictWildcard);
                        response.set(TokenType.TX_ABORT_CONFLICT);
                    }
                });
            }

            // otherwise, check for conflict based on streams updates
            else {
                UUID streamID = entry.getKey();
                streamTailToGlobalTailMap.compute(streamID, (k, v) -> {
                    if (v == null) {
                        return null;
                    }
                    if (v > txSnapshotTimestamp) {
                        log.debug("ABORT[{}] conflict-stream[{}](ts={})",
                                txInfo, Utils.toReadableID(streamID), v);
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
     * This returns information about the tail of the
     * log and/or streams without changing/allocating anything.
     *
     * @param msg
     * @param ctx
     * @param r
     */
    public void handleTokenQuery(CorfuPayloadMsg<TokenRequest> msg,
                                 ChannelHandlerContext ctx, IServerRouter r) {
        TokenRequest req = msg.getPayload();

        // sanity backward-compatibility assertion; TODO: remove
        if (req.getStreams().size() > 1) {
            log.error("TOKEN-QUERY[{}]", req.getStreams());
        }

        long maxStreamGlobalTail = Address.NON_EXIST;

        // see if this query is for a specific stream-tail
        if (req.getStreams().size() == 1) {
            UUID streamID = req.getStreams().iterator().next();

            if (streamTailToGlobalTailMap.get(streamID) != null)
                maxStreamGlobalTail = streamTailToGlobalTailMap.get(streamID);

            // if we don't have informatin about this stream tail because of fail-over,
                // return the global tail of the log
            else if (isFailoverSequencer)
                maxStreamGlobalTail = globalLogTail.get() - 1L;
        }

        // Collect the latest local offset for every streams in the request.
        // TODO: just get an empty/null map
        ImmutableMap.Builder<UUID, Long> responseStreamTails = ImmutableMap.builder();

/* TODO: remove this
        for (UUID id : req.getStreams()) {
            streamTailMap.compute(id, (k, v) -> {
                if (v == null) {
                    if (isFailoverSequencer)
                        responseStreamTails.put(k, Address.NO_BACKPOINTER);
                    else
                        responseStreamTails.put(k, Address.NON_ADDRESS);
                    return null;
                }
                responseStreamTails.put(k, v);
                return v;
            });

            // Compute the latest global offset across all streams.
            Long lastIssued = streamTailToGlobalTailMap.get(id);
            maxStreamGlobalTails = Math.max(maxStreamGlobalTails, lastIssued == null ? Address.NON_ADDRESS : lastIssued);
        }
*/

        // TODO: remove this
        // this is only a query; no saving needed!
        // saveStreamTailMap(streamTailMap);

        // If no streams are specified in the request, this value returns the last global token issued.
        long responseGlobalTail = (req.getStreams().size() == 0) ? globalLogTail.get() - 1 : maxStreamGlobalTail;
        Token token = new Token(responseGlobalTail, r.getServerEpoch());
        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                new TokenResponse(TokenType.NORMAL, token, Collections.emptyMap(), responseStreamTails.build())));
    }

    /**
     * Service an incoming request to reset the sequencer.
     */
    @ServerHandler(type=CorfuMsgType.RESET_SEQUENCER, opTimer=metricsPrefix + "reset")
    public synchronized void resetServer(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx, IServerRouter r,
                                         boolean isMetricsEnabled) {
        // TODO: remove this:
        //long restoredGlobalTail = restoreGlobalLogTail();
        //long initialToken = restoredGlobalTail > msg.getPayload() ? restoredGlobalTail : msg.getPayload();
        long initialToken = msg.getPayload();

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
            isFailoverSequencer = true;
            globalLogTail.set(initialToken);
            globalLogStart.set(initialToken);
            maxConflictWildcard = initialToken-1;
            conflictToGlobalTailCache.invalidateAll();
            // FIXME: Do not preserve Sequencer State.
            //streamTailMap.clear();
            //streamTailToGlobalTailMap.clear();
            //streamTailMap.putAll(restoreStreamTailMap());
            //streamTailToGlobalTailMap.putAll(restoreStreamTailToGlobalTailMap());
        }

        log.info("Sequencer reset with token = {}", initialToken);
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    /**
     * Service an incoming token request.
     */
    @ServerHandler(type=CorfuMsgType.TOKEN_REQ, opTimer=metricsPrefix + "token-req")
    public synchronized void tokenRequest(CorfuPayloadMsg<TokenRequest> msg,
                                          ChannelHandlerContext ctx, IServerRouter r,
                                          boolean isMetricsEnabled) {
        final long serverEpoch = r.getServerEpoch();
        TokenRequest req = msg.getPayload();

        if (req.getReqType() == TokenRequest.TK_QUERY) {
            MetricsUtils.incConditionalCounter(isMetricsEnabled, counterToken0, 1);
            handleTokenQuery(msg, ctx, r);
            return;
        } else {
            MetricsUtils.incConditionalCounter(isMetricsEnabled, counterTokenSum, req.getNumTokens());
        }

        // for raw log implementation, simply extend the global log tail and return the global-log token
        if (req.getReqType() == TokenRequest.TK_RAW) {
            Token token = new Token(globalLogTail.getAndAdd(req.getNumTokens()), serverEpoch);
            // TODO: remove this
            //saveGlobalLogTail(globalLogTail.get());
            r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                    new TokenResponse(TokenType.NORMAL, token, Collections.emptyMap(), Collections.emptyMap())));
            return;
        }

        // in the TK_TX request type, the sequencer is utilized for transaction conflict-resolution.
        // Token allocation is conditioned on commit.
        // First, we check if the transaction can commit.
        if (req.getReqType() == TokenRequest.TK_TX) {

            TokenType tokenType = txnCanCommit(req.getTxnResolution());
            if (tokenType != TokenType.NORMAL) {
                // If the txn aborts, then DO NOT hand out a token.
                Token token = new Token(-1L, serverEpoch);
                r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                        new TokenResponse(tokenType, token, Collections.emptyMap(), Collections.emptyMap())));
                return;
            }
        }

        // extend the tail of the global log by the requested # of tokens
        // currentTail is the first available position in the global log
        long currentTail = globalLogTail.getAndAdd(req.getNumTokens());
        long newTail = currentTail + req.getNumTokens();

        // for each streams:
        //   1. obtain the last back-pointer for this streams, if exists; -1L otherwise.
        //   2. record the new global tail as back-pointer for this streams.
        //   3. extend the tail by the requested # tokens.
        ImmutableMap.Builder<UUID, Long> backPointerMap = ImmutableMap.builder();
        ImmutableMap.Builder<UUID, Long> requestStreamTokens = ImmutableMap.builder();
        for (UUID id : req.getStreams()) {

            // step 1. and 2. (comment above)
            streamTailToGlobalTailMap.compute(id, (k, v) -> {
                if (v == null) {
                    if (!isFailoverSequencer)
                        backPointerMap.put(k, Address.NON_EXIST);
                    else {
                        backPointerMap.put(k, Address.NO_BACKPOINTER);
                    }
                    return newTail-1;
                } else {
                    backPointerMap.put(k, v);
                    return newTail-1;
                }
            });

            // step 3. (comment above)
            streamTailMap.compute(id, (k, v) -> {
                if (v == null) {
                    requestStreamTokens.put(k, req.getNumTokens() - 1L);
                    return req.getNumTokens() - 1L;
                }
                requestStreamTokens.put(k, v + req.getNumTokens());
                return v + req.getNumTokens();
            });
        }

        // FIXME: Do not preserve Sequencer State.
        //saveGlobalLogTail(globalLogTail.get());
        //saveStreamTailMap(streamTailMap);
        //saveStreamTailToGlobalTailMap(streamTailToGlobalTailMap);

        // update the cache of conflict parameters
        if (req.getTxnResolution() != null)
            req.getTxnResolution().getWriteConflictParams().entrySet()
                .stream()
                    // for each entry
                .forEach(txEntry ->
                        // and for each conflict param
                            txEntry.getValue().stream().forEach(conflictParam ->
                                    // insert an entry with the new timestamp
                                    // using the hash code based on the param
                                    // and the stream id.
                                    conflictToGlobalTailCache.put(
                                            getConflictHashCode(txEntry
                                                    .getKey(), conflictParam),
                                            newTail - 1)));

        log.trace("token {} backpointers {} stream-tokens {}",
                currentTail, backPointerMap.build(), requestStreamTokens.build());
        // return the token response with the new global tail, new streams tails, and the streams backpointers
        Token token = new Token(currentTail, serverEpoch);
        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                new TokenResponse(TokenType.NORMAL,
                        token,
                        backPointerMap.build(),
                        requestStreamTokens.build())));
    }

    private void saveGlobalLogTail(Long globalLogTail) {
        serverContext.getDataStore().put(Long.class, PREFIX_SEQUENCER, KEY_GLOBAL_LOG_TAIL, globalLogTail);
    }

    private Long restoreGlobalLogTail() {
        Long tail = serverContext.getDataStore().get(Long.class, PREFIX_SEQUENCER, KEY_GLOBAL_LOG_TAIL);
        return tail == null ? 0L : tail;
    }

    private void saveStreamTailMap(ConcurrentHashMap<UUID, Long> streamTailMap) {
        serverContext.getDataStore().put(Map.class, PREFIX_SEQUENCER, KEY_STREAM_TAIL_MAP, streamTailMap);
    }

    private ConcurrentHashMap<UUID, Long> restoreStreamTailMap() {
        ConcurrentHashMap map = serverContext.getDataStore().get(ConcurrentHashMap.class, PREFIX_SEQUENCER, KEY_STREAM_TAIL_MAP);
        if (map == null) return new ConcurrentHashMap<>();
        ConcurrentHashMap<UUID, Long> streamTailMap = new ConcurrentHashMap<>();
        map.forEach((uuid, aLong) -> streamTailMap.put(UUID.fromString((String)uuid), ((Double)aLong).longValue()));
        return streamTailMap;
    }

    private void saveStreamTailToGlobalTailMap(ConcurrentHashMap<UUID, Long> streamTailToGlobalTailMap) {
        serverContext.getDataStore().put(Map.class, PREFIX_SEQUENCER, KEY_STREAM_TAIL_TO_GLOBAL_TAIL_MAP, streamTailToGlobalTailMap);
    }

    private ConcurrentHashMap<UUID, Long> restoreStreamTailToGlobalTailMap() {
        ConcurrentHashMap map = serverContext.getDataStore().get(ConcurrentHashMap.class, PREFIX_SEQUENCER, KEY_STREAM_TAIL_TO_GLOBAL_TAIL_MAP);
        if (map == null) return new ConcurrentHashMap<>();
        ConcurrentHashMap<UUID, Long> streamTailToGlobalTailMap = new ConcurrentHashMap<>();
        map.forEach((uuid, aLong) -> streamTailToGlobalTailMap.put(UUID.fromString((String)uuid), ((Double)aLong).longValue()));
        return streamTailToGlobalTailMap;
    }
}
