package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelHandlerContext;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.util.Utils;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.corfudb.infrastructure.ServerContext.NON_LOG_ADDR_MAGIC;

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

    /**
     * The sequencer maintains information about log and streams:
     *
     *  - {@link SequencerServer::globalLogTail}:
     *      global log tail. points to the first available position (initially, 0).
     *  - {@link SequencerServer::streamTailMap}:
     *      a map of per-streams tail. points to per-streams first available position.
     *  - {@link SequencerServer::streamTailToGlobalTailMap}:
     *      per streams map to last issued global-log position. used for backpointers.
     *  - {@link SequencerServer::conflictToGlobalTailCache}:
     *      a cache of recent conflict keys and their latest global-log position
     *  - {@link SequencerServer::maxConflictWildcard} :
     *      a "wildcard" representing the maximal update timestamp of
     *      all the confict keys which were evicted from the cache
     *
     * Every append to the log updates the information in these maps.
     */
    @Getter
    private final AtomicLong globalLogTail = new AtomicLong(0L);
    private final ConcurrentHashMap<UUID, Long> streamTailMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Long> streamTailToGlobalTailMap = new ConcurrentHashMap<>();

    /**
     * an object representing a conflict-key on a specific stream
     */
    @Data
    class ConflictKey {
        final int NO_CONFLICT_KEY = -1;
        int streamIDhash;
        int conflictKeyhash;

        /**
         * construct a generic conflict-key for the stream.
         * the specific key will be set later
         * @param streamID
         */
        public ConflictKey(UUID streamID) {
            streamIDhash = streamID.hashCode();
            this.conflictKeyhash = NO_CONFLICT_KEY;
        }
    };

    private final long maxConflictCacheSize = 10_000;
    private long maxConflictWildcard = -1L;
    private final Cache<ConflictKey, Long>
            conflictToGlobalTailCache = Caffeine.newBuilder()
            .maximumSize(maxConflictCacheSize)
            .removalListener((ConflictKey K, Long V, RemovalCause cause) -> {
                maxConflictWildcard = Math.max(V, maxConflictWildcard);
            })
            .build();

    /** Handler for this server */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .generateHandlers(MethodHandles.lookup(), this);

    public SequencerServer(ServerContext serverContext) {
        this.serverContext = serverContext;
        this.opts = serverContext.getServerConfig();

        long initialToken = Utils.parseLong(opts.get("--initial-token"));
        if (initialToken == NON_LOG_ADDR_MAGIC) {
            globalLogTail.set(0L);
        } else {
            globalLogTail.set(initialToken);
        }
    }

    /**
     * Returns true if the txn commits.
     * If the request submits a timestamp (a global offset) that is less than one of the
     * global offsets of a streams specified in the request, then abort; otherwise commit.
     *
     * @param txData info provided by corfuRuntime for conflict resolultion:
     *              - timestamp : the snapshot (global) offset that this TX reads
     *              - conflictSet: conflict set of the txn.
     *                if any conflict-param (or stream, if empty) in this set has a later timestamp than the snapshot, abort
     */
    public boolean txnCanCommit(TxResolutionInfo txData) {
        log.trace("txn resolution, timestamp: {}, streams: {}", txData.getSnapshotTimestamp(), txData.getConflictSet());

        AtomicBoolean commit = new AtomicBoolean(true);
        for (Map.Entry<UUID, Set<Integer>> entry : txData.getConflictSet().entrySet()) {
            if (!commit.get())
                break;

            // if conflict-parameters are present, check for conflict based on conflict-parameter updates
            Set<Integer> conflictParamSet = entry.getValue();
            if (conflictParamSet != null && conflictParamSet.size() > 0) {

                // instantiate a key-pair, and set its streamID (1st component)
                ConflictKey conflictKeyPair =
                        new ConflictKey(entry.getKey());

                // for each key pair, check for conflict;
                // if not present, check against the wildcard
                conflictParamSet.forEach(conflictParam -> {
                    conflictKeyPair.setConflictKeyhash(conflictParam);
                    Long v = conflictToGlobalTailCache.getIfPresent(conflictKeyPair);
                    log.trace("txn resolution for conflictparam {}, last update {}",
                            conflictKeyPair, v);
                    if ((v != null && v > txData.getSnapshotTimestamp()) ||
                            (maxConflictWildcard > txData.getSnapshotTimestamp()) ) {
                        log.debug("Rejecting request due to update-timestamp " +
                                        "> {} on conflictKeyPair {}",
                                txData.getSnapshotTimestamp(), conflictKeyPair);
                        commit.set(false);
                    }
                });
            }

            // otherwise, check for conflict based on streams updates
            else {
                UUID streamID = entry.getKey();
                streamTailToGlobalTailMap.compute(streamID, (k, v) -> {
                    if (v == null) {
                        return null;
                    } else {
                        if (v > txData.getSnapshotTimestamp()) {
                            log.debug("Rejecting request due to {} > {} on streams {}",
                                    v, txData.getSnapshotTimestamp(), streamID);
                            commit.set(false);
                        }
                    }
                    return v;
                });
            }
        }

        return commit.get();
    }

    public void handleTokenQuery(CorfuPayloadMsg<TokenRequest> msg,
                                 ChannelHandlerContext ctx, IServerRouter r) {
        TokenRequest req = msg.getPayload();

        long maxStreamGlobalTails = -1L;

        // Collect the latest local offset for every streams in the request.
        ImmutableMap.Builder<UUID, Long> responseStreamTails = ImmutableMap.builder();

        for (UUID id : req.getStreams()) {
            streamTailMap.compute(id, (k, v) -> {
                if (v == null) {
                    responseStreamTails.put(k, -1L);
                    return null;
                }
                responseStreamTails.put(k, v);
                return v;
            });
            // Compute the latest global offset across all streams.
            Long lastIssued = streamTailToGlobalTailMap.get(id);
            maxStreamGlobalTails = Math.max(maxStreamGlobalTails, lastIssued == null ? Long.MIN_VALUE : lastIssued);
        }

        // If no streams are specified in the request, this value returns the last global token issued.
        long responseGlobalTail = (req.getStreams().size() == 0) ? globalLogTail.get() - 1 : maxStreamGlobalTails;
        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                new TokenResponse(responseGlobalTail, Collections.emptyMap(), responseStreamTails.build())));
    }

    /**
     * Service an incoming token request.
     */
    @ServerHandler(type=CorfuMsgType.TOKEN_REQ)
    public synchronized void tokenRequest(CorfuPayloadMsg<TokenRequest> msg,
                                          ChannelHandlerContext ctx, IServerRouter r) {
        TokenRequest req = msg.getPayload();

        if (req.getReqType() == TokenRequest.TK_QUERY) {
            handleTokenQuery(msg, ctx, r);
            return;
        }

        // for raw log implementation, simply extend the global log tail and return the global-log token
        if (req.getReqType() == TokenRequest.TK_RAW) {
            r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                    new TokenResponse(globalLogTail.getAndAdd(req.getNumTokens()), Collections.emptyMap(), Collections.emptyMap())));
            return;
        }

        // in the TK_TX request type, the sequencer is utilized for transaction conflict-resolution.
        // Token allocation is conditioned on commit.
        // First, we check if the transaction can commit.
        if (req.getReqType() == TokenRequest.TK_TX) {

            if (!txnCanCommit(req.getTxnResolution())) {
                // If the txn aborts, then DO NOT hand out a token.
                r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                        new TokenResponse(-1L, Collections.emptyMap(), Collections.emptyMap())));
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
                    backPointerMap.put(k, -1L);
                    return newTail-1;
                } else {
                    backPointerMap.put(k, v);

                    // legacy code, addition sanity check instead:
                    //return Math.max(newTail - 1, v);
                    if (newTail-1 < v)
                        log.error("backpointer {} is already greater than newTail-1 {}", v, newTail-1);

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

        // update the cache of conflict parameters
        if (req.getTxnResolution() != null)
            for (Map.Entry<UUID, Set<Integer>> writeConflictEntry :
                    req.getTxnResolution().getWriteConflictParams().entrySet()) {

                // instantiate a key-pair, and set its streamID (1st component)
                ConflictKey conflictKeyPair =
                        new ConflictKey(writeConflictEntry.getKey());

                // now update each key-pair with this token's timestamp
                for (Integer cParam : writeConflictEntry.getValue()) {
                    conflictKeyPair.setConflictKeyhash(cParam);
                    conflictToGlobalTailCache.put(conflictKeyPair, newTail - 1);
                }
            }

        log.debug("token {} backpointers {} stream-tokens {}", currentTail, backPointerMap.build(), requestStreamTokens.build());
        // return the token response with the new global tail, new streams tails, and the streams backpointers
        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                new TokenResponse(currentTail,
                        backPointerMap.build(),
                        requestStreamTokens.build())));
    }
}
