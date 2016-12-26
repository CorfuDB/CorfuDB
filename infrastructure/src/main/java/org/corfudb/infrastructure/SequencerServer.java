package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
     *  - {@link SequencerServer::streamTailMap}: a map of per-stream tail. points to per-stream first available position.
     *  - {@link SequencerServer::streamBackpointerMap}: per stream map to last issued global-log position. used for backpointers.
     *  - {@link SequencerServer::conflictToGlobalTailCache}: the {@link SequencerServer::maxConflictCacheSize} latest conflict keys and their latest commit (global-log) position
     *
     * Every append to the log updates the information in these maps.
     */
    @Getter
    private final AtomicLong globalLogTail = new AtomicLong(0L);
    private final ConcurrentHashMap<UUID, Long> streamTailMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Long> streamBackpointerMap = new ConcurrentHashMap<>();
    private final long maxConflictCacheSize = 10_000;
    private final Cache<Object, Long> conflictToGlobalTailCache = Caffeine.newBuilder()
            .maximumSize(maxConflictCacheSize)
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
     * global offsets of a stream specified in the request, then abort; otherwise commit.
     *
     * @param timestamp Read timestamp of the txn; in order to commit, no writes may be made past this
     *                  (global) timestamp on any streams touched by the txn.
     * @param streams   Read set of the txn.
     */
    public boolean txnResolution(long timestamp, Set<UUID> streams) {
        log.trace("txn resolution, timestamp: {}, streams: {}", timestamp, streams);

        AtomicBoolean commit = new AtomicBoolean(true);
        for (UUID id : streams) {
            if (!commit.get())
                break;


            streamBackpointerMap.compute(id, (k, v) -> {
                if (v == null) {
                    return null;
                } else {
                    if (v > timestamp) {
                        log.debug("Rejecting request due to {} > {} on stream {}", v, timestamp, id);
                        commit.set(false);
                    }
                }
                return v;
            });
        }
        return commit.get();
    }

    public void handleTokenQuery(CorfuPayloadMsg<TokenRequest> msg,
                                 ChannelHandlerContext ctx, IServerRouter r) {
        TokenRequest req = msg.getPayload();

        long maxStreamGlobalTails = -1L;

        // Collect the latest local offset for every stream in the request.
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
            Long lastIssued = streamBackpointerMap.get(id);
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

            if (!txnResolution(req.getTxnResolution().getReadTimestamp(), req.getTxnResolution().getReadSet())) {
                // If the txn aborts, then DO NOT hand out a token.
                r.sendResponse(ctx, msg, CorfuMsgType.TOKEN_RES.payloadMsg(
                        new TokenResponse(-1L, Collections.emptyMap(), Collections.emptyMap())));
                return;
            }
        }

        // extend the tail of the global log by the requested # of tokens
        // currentTail is the first available position in the global log
        long currentTail = globalLogTail.getAndAdd(req.getNumTokens());

        // for each stream:
        //   1. obtain the last back-pointer for this stream, if exists; -1L otherwise.
        //   2. record the new global tail as back-pointer for this stream.
        //   3. extend the tail by the requested # tokens.
        ImmutableMap.Builder<UUID, Long> backPointerMap = ImmutableMap.builder();
        ImmutableMap.Builder<UUID, Long> requestStreamTokens = ImmutableMap.builder();
        for (UUID id : req.getStreams()) {

            // step 1. and 2. (comment above)
            streamBackpointerMap.compute(id, (k, v) -> {
                if (v == null) {
                    backPointerMap.put(k, -1L);
                    return currentTail + req.getNumTokens() - 1;
                } else {
                    backPointerMap.put(k, v);
                    return Math.max(currentTail + req.getNumTokens() - 1, v);
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

        // return the token response with the new global tail, new stream tails, and the stream backpointers
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
