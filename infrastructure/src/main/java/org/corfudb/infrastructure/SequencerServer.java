package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelHandlerContext;
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
 * TOKEN_REQ - Request the next token.
 * <p>
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class SequencerServer extends AbstractServer {

    /**
     * key-name for storing {@link SequencerServer} state in {@link ServerContext::getDataStore()}.
     */
    private static final String PREFIX_SEQUENCER = "SEQUENCER";
    private static final String KEY_SEQUENCER = "CURRENT";

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
     *  - {@link SequencerServer::globalLogTail}: global log tail. points to the first available position (initially, 0).
     *  - {@link SequencerServer::streamTailMap}: a map of per-streams tail. points to per-streams first available position.
     *  - {@link SequencerServer::streamTailToGlobalTailMap}: per streams map to last issued global-log position. used for backpointers.
     *  - {@link SequencerServer::conflictToGlobalTailCache}: the {@link SequencerServer::maxConflictCacheSize} latest conflict keys and their latest commit (global-log) position
     *
     * Every append to the log updates the information in these maps.
     */
    @Getter
    private final AtomicLong globalLogTail = new AtomicLong(0L);
    private final ConcurrentHashMap<UUID, Long> streamTailMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Long> streamTailToGlobalTailMap = new ConcurrentHashMap<>();

    /**
     * A cache of conflict parameter modification timestamps.
     *
     * A "wildcard" {@link SequencerServer::maxConflictWildcard} representing all the entries which were evicted from the cache holds
     * theier maximal modification time.
     */
    private final long maxConflictCacheSize = 10_000;
    private long maxConflictWildcard = -1L;
    private final Cache<Integer, Long> conflictToGlobalTailCache = Caffeine.newBuilder()
            .maximumSize(maxConflictCacheSize)
            .removalListener((Integer  K, Long V, RemovalCause cause) -> {
                maxConflictWildcard = Math.max(V, maxConflictWildcard);
            })
            .build();

    /**
     * A sequencer needs a lease to serve a certain number of tokens.
     * The lease starting index is persisted.
     * A lease is good for (@Link #SequencerServer::leaseLength) number of tokens.
     *
     * A lease is renewed when we reach leaseRenew tokens away from the limit.
     *
     * TODO: these parameters should probably be configurable from somewhere
     */
    @Getter
    private final long leaseLength = 100_000;
    private final long leaseRenewalNotice = 10_000; // renew when token crosses leaseLength - leaseRenewalNotice threshold

    /** Handler for this server */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .generateHandlers(MethodHandles.lookup(), this);

    public SequencerServer(ServerContext serverContext) {
        this.serverContext = serverContext;
        this.opts = serverContext.getServerConfig();

        long initialToken = Utils.parseLong(opts.get("--initial-token"));
        if (initialToken == NON_LOG_ADDR_MAGIC) {
            getInitalLease();
        } else {
            renewLease(initialToken);
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
                conflictParamSet.forEach(conflictParam -> {
                    Long v = conflictToGlobalTailCache.getIfPresent(conflictParam);
                    if ((v != null && v > txData.getSnapshotTimestamp()) ||
                            (maxConflictWildcard > txData.getSnapshotTimestamp()) ) {
                        log.debug("Rejecting request due to update-timestamp > {} on conflictParam {}",
                                txData.getSnapshotTimestamp(), conflictParam);
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

        // check if log tail getting close to lease-limit. If so, we need to renew sequencer lease
        long leaseRenew = getCurrentLease() + leaseLength;
        if (globalLogTail.get() >= (leaseRenew - leaseRenewalNotice))
            renewLease(leaseRenew);

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
            for (Set<Integer> conflictParams : req.getTxnResolution().getWriteConflictParams().values())
                for (Integer cParam : conflictParams)
                    conflictToGlobalTailCache.put(cParam, newTail-1);

        log.debug("token {} backpointers {} stream-tokens {}", currentTail, backPointerMap.build(), requestStreamTokens.build());
        // return the token response with the new global tail, new streams tails, and the streams backpointers
        r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                new TokenResponse(currentTail,
                        backPointerMap.build(),
                        requestStreamTokens.build())));
    }

    /**
     * obtain the initial lease (a log tail).
     * for now, this works only with a local file.
     * TODO in the future, a sequencer needs to obtain the lease from the layout service
     */
    private void getInitalLease() {

        // check for existing previous lease
        Long leaseTail = serverContext.getDataStore()
                .get(Long.class, PREFIX_SEQUENCER, KEY_SEQUENCER);

        if (leaseTail != null) {
            // if a previous lease exists, go past it to teh next lease segment
            renewLease(leaseTail + leaseLength);
            globalLogTail.set(leaseTail + leaseLength);
            // todo: we need to update the conflictCache to reflect the lack of information up to the current tail
        } else {
            // otherwise, grab a lease from the start of the log
            renewLease(0L);
            globalLogTail.set(0L);
        }

    }

    /**
     * extend the current lease to a new tail
     * @param leaseStart the new lease starting point
     */
    private void renewLease(long leaseStart) {
        serverContext.getDataStore()
                .put(Long.class, PREFIX_SEQUENCER, KEY_SEQUENCER, leaseStart);
    }

    /**
     * query the current lease
     * @return the lease's starting point
     */
    private long getCurrentLease() {
        return serverContext.getDataStore()
                .get(Long.class, PREFIX_SEQUENCER, KEY_SEQUENCER);
    }
}
