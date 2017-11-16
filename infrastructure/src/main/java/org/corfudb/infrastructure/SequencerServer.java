package org.corfudb.infrastructure;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.StampedLock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
 * <p>The sequencer issues tokens for clients, which can be thought of as permits to write into
 * log addresses, on a per-stream basis. Clients may request a token for a single stream, multiple
 * streams, or no stream. Each successful token request increments a counter, referred to as the
 * {@code logTail}, by one. Clients may also query the latest token issued on a per-stream or global
 * basis. Finally, the sequencer also decides if transactions may commit, if the client provides
 * optional transaction resolution information. This information contains a set of hash codes a
 * operation conflicts with - the sequencer checks that each hash code was not modified between
 * a client provided address and the current {@logTail}. If the check passes, the sequencer issues
 * an address allowing the transaction to commit, otherwise, the sequencer aborts the transaction
 * without issuing an address.
 *
 * <p>The sequencer keeps track of five kinds of global state:
 *
 * <p>{@code logTail}, which is the current tail (last issued address) of the log.
 * During reads, clients query the {@code logTail} to obtain a linearization point,
 * and during writes, the sequencer increments {@code logTail}.
 *
 * <p>{@code trimMark}, which is the last address we have trimmed. Addresses below
 * this range may no longer be accessible (and can no longer be written to).
 *
 * <p>{@code streamTailMap}, which is a map from stream identifiers to their tails
 * (the last issued address for that stream).
 *
 * <p>{@code conflictToTailCache}, which is a cache of fine-grained conflict information
 * and the latest update for that conflict.
 *
 * <p>{@code maxConflictWildcard}, which marks the lower-bound of the address we have
 * valid conflict information for.
 *
 * <p>All fields are protected by a stampedLock ({@code lock}). It is safe to read (query) the
 * value of any single field without a lock, the Java memory model (JSR-133) and the volatile
 * keyword (where applicable, on long fields), guarantee that reads will not be cached and writes
 * from other threads will be visible to any read. However, writes or reads from multiple fields
 * will require the lock to be acquired. When possible, optimism (such as when validating
 * whether a transaction may commit) is used instead of acquiring a hard lock.
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
     * The current log tail.
     */
    @Getter
    private volatile long logTail;

    /**
     * The current trim mark. Addresses below this mark can no longer be written to
     * and may no longer be accessible.
     */
    private volatile long trimMark = Address.NON_ADDRESS;

    /**
     *  A map from stream identifiers to addresses ("stream tail").
     */
    private final MutableObjectLongMap<UUID> streamTailMap = new ObjectLongHashMap<>();

    /** A container for conflict objects, which combines the stream identifier with the
     * actual conflict object.
     */
    @Data
    public static class ConflictObject {
        /** The stream this conflict object belongs to. */
        final UUID streamId;

        /** The conflict key this object belongs to. */
        final byte[] conflictKey;
    }

    /**
     *  A cache of conflict keys to their latest position (last modification).
     */
    private final Cache<ConflictObject, Long> conflictToTailCache;

    /**
     * A lower bound indicating the minimum address the conflict cache is valid for.
     */
    private long maxConflictWildcard = Address.NOT_FOUND;


    /**
     * A stamped lock, which is used to protect writes to the above fields.
     */
    private final StampedLock lock = new StampedLock();

    /**
     * Handler for this server.
     */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .generateHandlers(MethodHandles.lookup(), this);

    private static final String metricsPrefix = "corfu.server.sequencer.";
    private static Counter counterTokenSum;
    private static Counter counterToken0;


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
            logTail = 0L;
        } else {
            logTail = initialToken;
        }

        MetricRegistry metrics = serverContext.getMetrics();
        counterTokenSum = metrics.counter(metricsPrefix + "token-sum");
        counterToken0 = metrics.counter(metricsPrefix + "token-query");

        long cacheSize = 250_000;
        if (opts.get("--sequencer-cache-size") != null) {
            cacheSize = Long.parseLong((String) opts.get("--sequencer-cache-size"));

        }

        conflictToTailCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .removalListener((ConflictObject k, Long v, RemovalCause cause) -> {
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
     * Handle a trim request, updating the trimMark.
     *
     * @param msg                   The trim request message.
     * @param ctx                   The incoming channel handler context.
     * @param r                     The server router handling the request.
     * @param isMetricsEnabled      Whether metrics are enabled or not.
     */
    @ServerHandler(type = CorfuMsgType.SEQUENCER_TRIM_REQ, opTimer = metricsPrefix + "trimCache")
    public void trimCache(@Nonnull CorfuPayloadMsg<Long> msg,
                          @Nonnull ChannelHandlerContext ctx,
                          IServerRouter r,
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
            for (Map.Entry<ConflictObject, Long> entry : conflictToTailCache.asMap().entrySet()) {
                if (entry.getValue() < trimMark) {
                    conflictToTailCache.invalidate(entry.getKey());
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
     * Service an incoming request to bootstrap the sequencer.
     *
     * @param msg                   The bootstrap message.
     * @param ctx                   The incoming channel handler context.
     * @param r                     The server router handling the request.
     * @param isMetricsEnabled      Whether metrics are enabled or not.
     */
    @ServerHandler(type = CorfuMsgType.BOOTSTRAP_SEQUENCER, opTimer = metricsPrefix + "bootstrap")
    public void bootstrapServer(CorfuPayloadMsg<SequencerTailsRecoveryMsg> msg,
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
                        + "Discarding bootstrap request with epoch {}", readyStateEpoch,
                        readyEpoch);
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
            if (initialToken > logTail) {
                logTail = initialToken;
                maxConflictWildcard = initialToken - 1;
                conflictToTailCache.invalidateAll();

                // Clear the existing map as it could have been populated by an earlier reset.
                streamTailMap.clear();
                streamTails.entrySet()
                        .forEach(e -> streamTailMap.put(e.getKey(), e.getValue()));
            }

            // Mark the sequencer as ready after the tails have been populated.
            readyStateEpoch = readyEpoch;

            log.info("Sequencer reset with token = {}, streamTailMap = {},"
                            + " readyStateEpoch = {}",
                    initialToken, streamTailMap, readyStateEpoch);
        } finally {
            lock.unlock(ts);
        }
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }


    /**
     * Service an incoming request to bootstrap the sequencer.
     *
     * @param msg                   The token request message.
     * @param ctx                   The incoming channel handler context.
     * @param r                     The server router handling the request.
     * @param isMetricsEnabled      Whether metrics are enabled or not.
     */
    @ServerHandler(type = CorfuMsgType.TOKEN_REQ, opTimer = metricsPrefix + "token-req")
    public void tokenRequest(CorfuPayloadMsg<TokenRequest> msg,
                             ChannelHandlerContext ctx, IServerRouter r,
                             boolean isMetricsEnabled) {
        final TokenRequest req = msg.getPayload();
        final long serverEpoch = r.getServerEpoch();

        // metrics collection
        if (req.getReqType() == TokenRequest.TK_QUERY) {
            MetricsUtils.incConditionalCounter(isMetricsEnabled, counterToken0, 1);
        } else {
            MetricsUtils.incConditionalCounter(isMetricsEnabled, counterTokenSum, 1);
        }

        CorfuMsg response;
        // dispatch request handler according to request type
        switch (req.getReqType()) {
            case TokenRequest.TK_QUERY:
                response = handleTokenQuery(req, serverEpoch);
                break;
            case TokenRequest.TK_QUERY_STREAM:
                response = handleStreamTokenQuery(req, serverEpoch);
                break;
            case TokenRequest.TK_RAW:
                response = handleRawToken(req, serverEpoch);
                break;
            case TokenRequest.TK_TX:
                response = handleTxToken(req, serverEpoch);
                break;
            default:
                long ts = lock.writeLock();
                try {
                    response = handleAllocationUnsafe(req, req.getTxnResolution(), serverEpoch);
                    break;
                } finally {
                    lock.unlock(ts);
                }
        }

        r.sendResponse(ctx, msg, response);
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
     * Service a query request.
     * This method returns the logTail without allocating anything.
     *
     * @param request       The incoming token request (ignored).
     * @param serverEpoch   The incoming server epoch.
     * @return              A {@link CorfuMsg} to respond with.
     */
    private CorfuMsg handleTokenQuery(@Nonnull TokenRequest request,
                                      long serverEpoch) {
        return CorfuMsgType.TOKEN_RES.payloadMsg(
                new TokenResponse(logTail - 1, serverEpoch));
    }


    /**
     * Service a query request for a stream tail.
     * This method returns the tail of a stream without allocating anything.
     *
     * @param request       The incoming token request.
     * @param serverEpoch   The incoming server epoch.
     * @return              A {@link CorfuMsg} to respond with.
     */
    private CorfuMsg handleStreamTokenQuery(@Nonnull TokenRequest request,
                                       long serverEpoch) {
        final int numStreams = request.getStreams().size();
        long maxStreamGlobalTail = Address.NON_EXIST;

        // see if this query is for a specific stream-tail
        if (numStreams == 1) {
            UUID streamId = request.getStreams().get(0);

            long ts = lock.tryOptimisticRead();
            if (ts != 0) {
                maxStreamGlobalTail =
                        streamTailMap.getIfAbsent(streamId, Address.NON_EXIST);
            }
            while (maxStreamGlobalTail == Address.NON_EXIST && !lock.validate(ts)) {
                ts = lock.tryOptimisticRead();
                maxStreamGlobalTail = streamTailMap
                        .getIfAbsent(streamId, Address.NON_EXIST);
            }
        }
        return CorfuMsgType.TOKEN_RES.payloadMsg(new TokenResponse(maxStreamGlobalTail,
                serverEpoch));
    }


    /**
     * Service a request for a raw (stream-less) token.
     *
     * @param request       The incoming token request.
     * @param serverEpoch   The incoming server epoch.
     * @return              A {@link CorfuMsg} to respond with.
     */
    private CorfuMsg handleRawToken(@Nonnull TokenRequest request,
                                    long serverEpoch) {
        long prevTail;
        long ts = lock.writeLock();
        try {
            prevTail = logTail++;
        } finally {
            lock.unlock(ts);
        }

        return CorfuMsgType.TOKEN_RES.payloadMsg(
                new TokenResponse(prevTail, serverEpoch));

    }


    /**
     * Service a requests for transaction commit.
     * This method checks if a transaction can commit by calling
     * {@link this#verifyCommitUnsafe(long, TxResolutionInfo, long)}.
     * If verification passes, then a token is allocated by
     * {@link this#handleAllocationUnsafe(TokenRequest, TxResolutionInfo, long)}. Otherwise, the
     * transaction is aborted by returning the failure message returned by
     * {@link this#verifyCommitUnsafe(long, TxResolutionInfo, long)}.
     *
     * @param request       The incoming token request.
     * @param serverEpoch   The incoming server epoch.
     * @return              A {@link CorfuMsg} to respond with.
     */
    private CorfuMsg handleTxToken(@Nonnull final TokenRequest request,
                                   long serverEpoch) {
        final TxResolutionInfo txInfo = request.getTxnResolution();
        final long txSnapshotTimestamp = txInfo.getSnapshotTimestamp();

        // If the tx has no snapshot (conflict-free) then issue the tokens.
        if (txSnapshotTimestamp == Address.NO_SNAPSHOT) {
            log.debug("handleTxToken[{}]: Conflict-free TX");
            long ts = lock.writeLock();
            try {
                return handleAllocationUnsafe(request, txInfo, serverEpoch);
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
        try {
            ts = lock.tryConvertToWriteLock(ts);
            if (ts != 0) {
                return handleAllocationUnsafe(request, txInfo, serverEpoch);
            } else {
                // Someone else must have grabbed the lock in the meantime.
                // Get a write-lock, revalidate and allocate if safe.
                ts = lock.writeLock();
                abortMsg = verifyCommitUnsafe(serverEpoch, txInfo, txSnapshotTimestamp);
                if (abortMsg != null) {
                    return abortMsg;
                }
                return handleAllocationUnsafe(request, txInfo, serverEpoch);
            }
        } finally {
            lock.unlock(ts);
        }
    }

    /** Verify whether a transaction may commit. Unsafe, but does not modify any fields.
     *  Running without a lock may produce false positives, but never false abort, since
     *  counters increment only monotonically, and any abort is guaranteed to hold despite
     *  concurrent writers.
     *
     * @param serverEpoch           The server epoch.
     * @param txInfo                The transaction information from the client.
     * @param txSnapshotTimestamp   The snapshot to do verification from.
     * @return                      A {@link CorfuMsg} with abort information if verification fails,
     *                              otherwise null if the transaction is safe to proceed.
     */
    private @Nullable CorfuMsg verifyCommitUnsafe(final long serverEpoch,
                                @Nonnull final TxResolutionInfo txInfo,
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
                    final ConflictObject conflictKeyHash =
                        new ConflictObject(streamId, conflictParam);
                    final Long conflictTail = conflictToTailCache
                            .getIfPresent(conflictKeyHash);
                    final Long validatedAddress =
                            txInfo.getValidatedStreams().get(streamId);

                    if (txSnapshotTimestamp < maxConflictWildcard) {
                        log.debug("verifyCommitUnsafe: ABORT[{}] snapshot-ts[{}] WILDCARD ts=[{}]",
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
                            log.debug("verifyCommitUnsafe: ABORT[{}] {}conflict-key[{}]({}ts={})",
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
                            log.warn("verifyCommitUnsafe: validated stream {} {} overrides {}",
                                    Utils.toReadableId(streamId),
                                    validatedAddress,
                                    conflictTail);
                        }
                    }
                    log.trace("verifyCommitUnsafe: OK[{}] conflict-key[{}](ts={})", txInfo,
                            conflictParam, conflictTail);
                }
            } else { // otherwise, check for conflict based on streams updates
                final Long streamTail = streamTailMap.get(streamId);
                if (streamTail > txSnapshotTimestamp) {
                    log.debug("verifyCommitUnsafe: ABORT[{}] conflict-stream[{}](ts={})",
                            txInfo, Utils.toReadableId(streamId), streamTail);
                    return
                            CorfuMsgType.TOKEN_RES.payloadMsg(
                                    new TokenResponse(TokenType.TX_ABORT_CONFLICT_STREAM,
                                            TokenResponse.NO_CONFLICT_KEY,
                                            streamId, streamTail, serverEpoch));
                }
                log.trace("verifyCommitUnsafe: OK[{}] conflict-stream[{}](ts={})", txInfo,
                        Utils.toReadableId(streamId), streamTail);
            }
        }
        return null;
    }

    /** Handle allocation of new tokens and update sequencer data structures. Unsafe, so must
     * be run under a write lock.
     *
     * @param request        The incoming token request.
     * @param resolutionInfo Optional transaction resolution information.
     * @param serverEpoch    The incoming server epoch.
     * @return              A {@link CorfuMsg} with allocation information, including backpointers.
     */
    private CorfuMsg handleAllocationUnsafe(@Nonnull TokenRequest request,
                                            @Nullable TxResolutionInfo resolutionInfo,
                                            long serverEpoch) {

        // extend the tail of the global log by the requested # of tokens
        // currentTail is the first available position in the global log
        final long currentTail = logTail++;

        final List<UUID> streams = request.getStreams();
        // for each streams:
        //   1. obtain the last back-pointer for this streams, if exists; -1L otherwise.
        //   2. record the new global tail as back-pointer for this streams.
        //   3. extend the tail by the requested # tokens.
        BackpointerResponse backPointerMap = new BackpointerResponse(streams.size());

        streams
                .forEach(id -> {
                    streamTailMap.updateValue(
                                    id,
                                    Address.NON_EXIST,
                            v -> {
                                backPointerMap.add(id, v);
                                return currentTail;
                            });

                });

        // update the cache of conflict parameters
        if (resolutionInfo != null) {
            // Cache the Long value.
            final Long currentTailBoxed = currentTail;
            resolutionInfo.getWriteConflictParams()
                    .entrySet()
                    .stream()
                    .flatMap(e ->
                            e.getValue().stream().map(conflictParam ->
                                    new ConflictObject(e.getKey(), conflictParam)))
                    .forEach(obj -> conflictToTailCache.put(obj, currentTailBoxed));
        }

        log.trace("handleAllocationUnsafe[{}]: backpointers {}", currentTail, backPointerMap);
        // return the token response with the new global tail
        // and the streams backpointers
        return CorfuMsgType.TOKEN_RES.payloadMsg(new TokenResponse(
                currentTail, serverEpoch, backPointerMap));
    }

    @VisibleForTesting
    public Cache<ConflictObject, Long> getConflictToTailCache() {
        return conflictToTailCache;
    }
}
