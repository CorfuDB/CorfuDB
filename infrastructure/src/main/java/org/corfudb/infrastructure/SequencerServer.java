package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.TextFormat;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.infrastructure.SequencerServerCache.ConflictTxStream;
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.SequencerMetrics.SequencerStatus;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TokenType;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.proto.RpcCommon.StreamAddressRangeMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidToStreamAddressSpacePairMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.StreamsAddressRequestMsg;
import org.corfudb.runtime.proto.service.Sequencer.TokenRequestMsg;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.Utils;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;


import static org.corfudb.protocols.CorfuProtocolCommon.getStreamAddressRange;
import static org.corfudb.protocols.CorfuProtocolCommon.getStreamsAddressResponseMsg;
import static org.corfudb.protocols.CorfuProtocolCommon.getStreamAddressSpace;
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.CorfuProtocolTxResolution.getTxResolutionInfo;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getBootstrapSequencerResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getSequencerMetricsResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getSequencerTrimResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getTokenResponseMsg;

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
 * {@link TokenRequestMsg.TokenRequestType}:
 *
 * <p>{@link TokenRequestMsg.TokenRequestType#TK_QUERY} - used for only querying the current tail
 * of the log and/or the tails of specific streams
 *
 * <p>{@link TokenRequestMsg.TokenRequestType#TK_RAW} - reserved for getting a "raw" token in the
 * global log
 *
 * <p>{@link TokenRequestMsg.TokenRequestType#TK_MULTI_STREAM} - used for logging across one or
 * more streams
 *
 * <p>{@link TokenRequestMsg.TokenRequestType#TK_TX} - used for reserving an address for transaction
 * commit.
 *
 * <p>The transaction commit is the most sophisticated functionality of the
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
     * Per streams map and their corresponding address space
     * (an address space is defined by the stream's addresses
     * and its latest trim mark)
     */
    private Map<UUID, StreamAddressSpace> streamsAddressMap;

    private long trimMark = Address.NON_ADDRESS;

    /**
     * - {@link SequencerServer::streamTailToGlobalTailMap}:
     * per streams map to last issued global-log position. used for backpointers.
     */
    private Map<UUID, Long> streamTailToGlobalTailMap;

    private final SequencerServerInitializer sequencerFactoryHelper;

    /**
     * RequestHandlerMethods for the Sequencer server
     */
    @Getter
    private final RequestHandlerMethods handlerMethods =
            RequestHandlerMethods.generateHandler(MethodHandles.lookup(), this);

    @Getter
    private SequencerServerCache cache;

    @Getter
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
     * - {@link SequencerServer::globalLogTail}:
     * global log first available position (initially, 0).
     */
    @Getter
    private long globalLogTail;

    /**
     * Note: This setter method is only used for testing, since we want to
     * keep sequencerEpoch private volatile.
     */
    @VisibleForTesting
    void setSequencerEpoch(long newEpoch) {
        sequencerEpoch = newEpoch;
    }

    /**
     * Returns a new SequencerServer.
     *
     * @param serverContext context object providing parameters and objects
     */
    public SequencerServer(ServerContext serverContext) {
        this(serverContext, new SequencerServerInitializer());
    }

    /**
     * Returns a new SequencerServer.
     *
     * @param serverContext          context object providing parameters and objects
     * @param sequencerFactoryHelper FactoryHelper providing utility methods to get various objects
     *                               in a factory pattern
     */
    public SequencerServer(ServerContext serverContext,
                           SequencerServerInitializer sequencerFactoryHelper) {
        this.serverContext = serverContext;
        this.sequencerFactoryHelper = sequencerFactoryHelper;

        // Sequencer server is single threaded by current design
        executor = serverContext.getExecutorService(1, "sequencer-");

        globalLogTail = sequencerFactoryHelper.getGlobalLogTail();
        cache = sequencerFactoryHelper.getSequencerServerCache(
                serverContext.getConfiguration().getSequencerCacheSize(),
                globalLogTail - 1
        );
        streamsAddressMap = sequencerFactoryHelper.getStreamAddressSpaceMap();
        streamTailToGlobalTailMap = sequencerFactoryHelper.getStreamTailToGlobalTailMap();
    }

    @Override
    protected void processRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        executor.submit(() -> getHandlerMethods().handle(req, ctx, r));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
    }

    @Override
    public boolean isServerReadyToHandleMsg(RequestMsg request) {
        if (getState() != ServerState.READY) {
            return false;
        }

        if ((sequencerEpoch != serverContext.getServerEpoch()) &&
                (!request.getPayload().getPayloadCase()
                        .equals(PayloadCase.BOOTSTRAP_SEQUENCER_REQUEST))) {

            log.warn("Rejecting msg at sequencer : sequencerStateEpoch:{}, serverEpoch:{}, "
                    + "header:{}", sequencerEpoch, serverContext.getServerEpoch(),
                    TextFormat.shortDebugString(request.getHeader()));
            return false;
        }
        return true;
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
        if (log.isTraceEnabled()) {
            log.trace("Commit-req[{}]", txInfo);
        }
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
            log.debug("ABORT[{}] snapshot-ts[{}] trimMark-ts[{}]",
                    txInfo, txSnapshotTimestamp, trimMark);
            return new TxResolutionResponse(TokenType.TX_ABORT_SEQ_TRIM);
        }
        MicroMeterUtils.measure(txInfo.getConflictSet().size(), "sequencer.tx-resolution.num_streams");
        for (Map.Entry<UUID, Set<byte[]>> conflictStream : txInfo.getConflictSet().entrySet()) {

            // if conflict-parameters are present, check for conflict based on conflict-parameter
            // updates
            Set<byte[]> conflictParamSet = conflictStream.getValue();
            //check for conflict based on streams updates
            if (conflictParamSet == null || conflictParamSet.isEmpty()) {
                UUID streamId = conflictStream.getKey();
                Long sequence = streamTailToGlobalTailMap.get(streamId);
                if (sequence != null && sequence > txSnapshotTimestamp.getSequence()) {
                    log.debug("ABORT[{}] conflict-stream[{}](ts={})",
                            txInfo, Utils.toReadableId(streamId), sequence);
                    return new TxResolutionResponse(TokenType.TX_ABORT_CONFLICT);
                }
                continue;
            }

            // for each key pair, check for conflict; if not present, check against the wildcard
            for (byte[] conflictParam : conflictParamSet) {

                Long keyAddress = cache.get(new ConflictTxStream(conflictStream.getKey(),
                        conflictParam, Address.NON_ADDRESS));

                if (log.isTraceEnabled()){
                    log.trace("Commit-ck[{}] conflict-key[{}](ts={})",
                            txInfo, conflictParam, keyAddress);
                }

                if (keyAddress > txSnapshotTimestamp.getSequence()) {
                    log.debug("ABORT[{}] conflict-key[{}](ts={})",
                            txInfo, conflictParam, keyAddress);
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
     * @param req corfu message containing token query
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleTokenQuery(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        final TokenRequestMsg tokenRequest = req.getPayload().getTokenRequest();
        final List<UUID> streams = tokenRequest.getStreamsList()
                .stream()
                .map(CorfuProtocolCommon::getUUID)
                .collect(Collectors.toList());

        Map<UUID, Long> streamTails;
        Token token;

        if (streams.isEmpty()) {
            // Global tail query
            token = new Token(sequencerEpoch, globalLogTail - 1);
            streamTails = Collections.emptyMap();
        } else {
            // multiple or single stream query, the token is populated with the global tail
            // and the tail queries are stored in streamTails
            token = new Token(sequencerEpoch, globalLogTail - 1);
            streamTails = new HashMap<>(streams.size());
            for (UUID stream : streams) {
                streamTails.put(stream,
                        streamTailToGlobalTailMap.getOrDefault(stream, Address.NON_EXIST));
            }
        }

        // Note: we reuse the request header as the ignore_cluster_id and
        // ignore_epoch fields are the same in both cases.
        ResponseMsg response = getResponseMsg(
                getHeaderMsg(req.getHeader()),
                getTokenResponseMsg(TokenType.NORMAL,
                        TokenResponse.NO_CONFLICT_KEY,
                        TokenResponse.NO_CONFLICT_STREAM,
                        token,
                        Collections.emptyMap(),
                        streamTails)
        );

        r.sendResponse(response, ctx);
    }

    @RequestHandler(type = PayloadCase.SEQUENCER_TRIM_REQUEST)
    public void trimCache(@Nonnull RequestMsg req,
                          @Nonnull ChannelHandlerContext ctx,
                          @Nonnull IServerRouter r) {
        log.info("trimCache: Starting cache eviction");
        if (trimMark < req.getPayload().getSequencerTrimRequest().getTrimMark()) {
            // Advance the trim mark, if the new trim request has a higher trim mark.
            trimMark = req.getPayload().getSequencerTrimRequest().getTrimMark();
            cache.invalidateUpTo(trimMark);

            // Remove trimmed addresses from each address map and set new trim mark
            for (StreamAddressSpace streamAddressSpace : streamsAddressMap.values()) {
                streamAddressSpace.trim(trimMark);
            }
        }

        log.debug("trimCache: global trim {}, streamsAddressSpace {}", trimMark, streamsAddressMap);

        HeaderMsg responseHeader = getHeaderMsg(req.getHeader(),
                ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        ResponseMsg response = getResponseMsg(responseHeader, getSequencerTrimResponseMsg());
        r.sendResponse(response, ctx);
    }

    /**
     * Service an incoming request to reset the sequencer.
     */
    @RequestHandler(type = PayloadCase.BOOTSTRAP_SEQUENCER_REQUEST)
    public void resetServer(@Nonnull RequestMsg req,
                            @Nonnull ChannelHandlerContext ctx,
                            @Nonnull IServerRouter r) {
        log.info("Reset sequencer server.");

        // Converting from addressSpaceMap object from Protobuf to Java as
        // this.streamsAddressMap needs java objects as arguments putAll()
        final Map<UUID, StreamAddressSpace> addressSpaceMap = req.getPayload()
                .getBootstrapSequencerRequest()
                .getStreamsAddressMapList()
                .stream()
                .collect(Collectors.<UuidToStreamAddressSpacePairMsg, UUID, StreamAddressSpace>
                        toMap(e -> getUUID(e.getStreamUuid()),
                        e -> getStreamAddressSpace(e.getAddressSpace()))
                );

        final long bootstrapMsgEpoch =
                req.getPayload().getBootstrapSequencerRequest().getSequencerEpoch();

        // Boolean flag to denote whether this bootstrap message is just updating an existing
        // primary sequencer with the new epoch (if set to true) or bootstrapping a currently
        // NOT_READY sequencer.
        final boolean bootstrapWithoutTailsUpdate = req.getPayload()
                .getBootstrapSequencerRequest().getBootstrapWithoutTailsUpdate();

        // If sequencerEpoch is -1 (startup) OR bootstrapMsgEpoch is not the consecutive epoch of
        // the sequencerEpoch then the sequencer should not accept bootstrapWithoutTailsUpdate
        // bootstrap messages.
        if (bootstrapWithoutTailsUpdate
                && (sequencerEpoch == Layout.INVALID_EPOCH
                || bootstrapMsgEpoch != sequencerEpoch + 1)) {

            log.warn("Cannot update existing sequencer. Require full bootstrap." +
                    " SequencerEpoch : {}, MsgEpoch : {}", sequencerEpoch, bootstrapMsgEpoch
            );

            // Note: we reuse the request header as the ignore_cluster_id and
            // ignore_epoch fields are the same in both cases.
            r.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()),
                    getBootstrapSequencerResponseMsg(false)), ctx);
            return;
        }

        // Stale bootstrap request should be discarded.
        if (serverContext.getSequencerEpoch() >= bootstrapMsgEpoch) {
            log.info("Sequencer already bootstrapped at epoch {}. " +
                            "Discarding bootstrap request with epoch {}",
                    sequencerEpoch, bootstrapMsgEpoch
            );

            // Note: we reuse the request header as the ignore_cluster_id and
            // ignore_epoch fields are the same in both cases.
            r.sendResponse(getResponseMsg(getHeaderMsg(req.getHeader()),
                    getBootstrapSequencerResponseMsg(false)), ctx);
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
            globalLogTail = req.getPayload().getBootstrapSequencerRequest().getGlobalTail();
            // Deregister gauges
            MeterRegistryProvider.deregisterServerMeter(cache.getConflictKeysCounterName(),
                    Tags.empty(), Meter.Type.GAUGE);
            MeterRegistryProvider.deregisterServerMeter(cache.getWindowSizeName(),
                    Tags.empty(), Meter.Type.GAUGE);
            cache = sequencerFactoryHelper.getSequencerServerCache(
                    cache.getCacheSize(),
                    globalLogTail - 1
            );

            // Clear the existing map as it could have been populated by an earlier reset.
            streamTailToGlobalTailMap = new HashMap<>();

            // Set tail for every stream
            for (Map.Entry<UUID, StreamAddressSpace> streamAddressSpace :
                    addressSpaceMap.entrySet()) {
                Long streamTail = streamAddressSpace.getValue().getTail();
                if (log.isTraceEnabled()) {
                    log.trace("On Sequencer reset, tail for stream {} set to {}",
                            streamAddressSpace.getKey(), streamTail);
                }
                streamTailToGlobalTailMap.put(streamAddressSpace.getKey(), streamTail);
            }

            // Reset streams address map
            streamsAddressMap = new HashMap<>();
            streamsAddressMap.putAll(addressSpaceMap);

            for (Map.Entry<UUID, StreamAddressSpace> streamAddressSpace :
                    streamsAddressMap.entrySet()) {
                log.info("Stream[{}]={} on sequencer reset.",
                        Utils.toReadableId(streamAddressSpace.getKey()),
                        streamAddressSpace.getValue());
                if (log.isTraceEnabled()) {
                    log.trace("Stream[{}] addresses: {}",
                            Utils.toReadableId(streamAddressSpace.getKey()),
                            streamAddressSpace.getValue().toArray());
                }
            }
        }

        // Update epochRangeLowerBound if the bootstrap epoch is not consecutive.
        if (epochRangeLowerBound == Layout.INVALID_EPOCH
                || bootstrapMsgEpoch != sequencerEpoch + 1) {
            epochRangeLowerBound = bootstrapMsgEpoch;
        }

        // Mark the sequencer as ready after the tails have been populated.
        sequencerEpoch = bootstrapMsgEpoch;
        serverContext.setSequencerEpoch(bootstrapMsgEpoch);

        log.info("Sequencer reset with token = {}, size {} streamTailToGlobalTailMap = {}," +
                " sequencerEpoch = {}", globalLogTail, streamTailToGlobalTailMap.size(),
                streamTailToGlobalTailMap, sequencerEpoch);

        HeaderMsg responseHeader = getHeaderMsg(req.getHeader(),
                ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        r.sendResponse(getResponseMsg(responseHeader,
                getBootstrapSequencerResponseMsg(true)), ctx);
    }

    /**
     * Service an incoming metrics request with the metrics response.
     */
    @RequestHandler(type = PayloadCase.SEQUENCER_METRICS_REQUEST)
    public void handleMetricsRequest(@Nonnull RequestMsg req,
                                     @Nonnull ChannelHandlerContext ctx,
                                     @Nonnull IServerRouter r) {
        // Sequencer Ready flag is set to true as this message will be responded to only if the
        // sequencer is in a ready state.
        // Note: we reuse the request header as the ignore_cluster_id and
        // ignore_epoch fields are the same in both cases.
        ResponseMsg response = getResponseMsg(getHeaderMsg(req.getHeader()),
                getSequencerMetricsResponseMsg(new SequencerMetrics(SequencerStatus.READY)));

        r.sendResponse(response, ctx);
    }

    /**
     * Service an incoming token request.
     */
    @RequestHandler(type = PayloadCase.TOKEN_REQUEST)
    public void tokenRequest(@Nonnull RequestMsg req,
                             @Nonnull ChannelHandlerContext ctx,
                             @Nonnull IServerRouter r) {
        if (log.isTraceEnabled()) {
            log.trace("tokenRequest: Token request msg: {}", TextFormat.shortDebugString(req));
        }
        final TokenRequestMsg tokenRequest = req.getPayload().getTokenRequest();

        // dispatch request handler according to request type while collecting the timer metrics
        switch (tokenRequest.getRequestType()) {
            case TK_QUERY:
                handleTokenQuery(req, ctx, r);
                break;

            case TK_RAW:
                handleRawToken(req, ctx, r);
                break;

            case TK_TX:
                handleTxToken(req, ctx, r);
                break;

            default:
                handleAllocation(req, ctx, r);
                break;
        }
    }

    /**
     * this method serves log-tokens for a raw log implementation.
     * it simply extends the global log tail and returns the global-log token
     *
     * @param req corfu message containing raw token
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    private void handleRawToken(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        final TokenRequestMsg tokenRequest = req.getPayload().getTokenRequest();

        // The global tail points to an open slot, not the last written slot,
        // so return the new token with current global tail and then update it.
        Token token = new Token(sequencerEpoch, globalLogTail);
        globalLogTail += tokenRequest.getNumTokens();

        // Note: we reuse the request header as the ignore_cluster_id and
        // ignore_epoch fields are the same in both cases.
        ResponseMsg response = getResponseMsg(getHeaderMsg(req.getHeader()),
                getTokenResponseMsg(token, Collections.emptyMap()));
        r.sendResponse(response, ctx);
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
    private void handleTxToken(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        final TokenRequestMsg tokenRequest = req.getPayload().getTokenRequest();

        // in the TK_TX request type, the sequencer is utilized for transaction conflict-resolution.
        // Token allocation is conditioned on commit.
        // First, we check if the transaction can commit.
        Supplier<TxResolutionResponse> txResponseSupplier =
                () -> txnCanCommit(getTxResolutionInfo(tokenRequest.getTxnResolution()));
        TxResolutionResponse txResolutionResponse =
                MicroMeterUtils.time(txResponseSupplier, "sequencer.tx-resolution.timer");

        if (txResolutionResponse.getTokenType() != TokenType.NORMAL) {
            // If the txn aborts, then DO NOT hand out a token.
            Token newToken = new Token(sequencerEpoch, txResolutionResponse.getAddress());

            // Note: we reuse the request header as the ignore_cluster_id and
            // ignore_epoch fields are the same in both cases.
            ResponseMsg response = getResponseMsg(getHeaderMsg(req.getHeader()), getTokenResponseMsg(
                    txResolutionResponse.getTokenType(),
                    txResolutionResponse.getConflictingKey(),
                    txResolutionResponse.getConflictingStream(),
                    newToken, Collections.emptyMap(), Collections.emptyMap()));

            r.sendResponse(response, ctx);
            return;
        }

        // if we get here, this means the transaction can commit.
        // handleAllocation() does the actual allocation of log position(s)
        // and returns the response
        handleAllocation(req, ctx, r);
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
    private void handleAllocation(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        final TokenRequestMsg tokenRequest = req.getPayload().getTokenRequest();

        // extend the tail of the global log by the requested # of tokens
        // currentTail is the first available position in the global log
        long newTail = globalLogTail + tokenRequest.getNumTokens();

        // for each stream:
        //   1. obtain the last back-pointer for this stream, if exists; -1L otherwise.
        //   2. record the new global tail as back-pointer for this stream.
        //   3. Add the allocated addresses to each stream's address map.
        ImmutableMap.Builder<UUID, Long> backPointerMap = ImmutableMap.builder();
        for (UuidMsg id : tokenRequest.getStreamsList()) {
            UUID uuid = getUUID(id);

            // step 1. and 2. (comment above)
            streamTailToGlobalTailMap.compute(uuid, (k, v) -> {
                if (v == null) {
                    backPointerMap.put(k, Address.NON_EXIST);
                } else {
                    backPointerMap.put(k, v);
                }
                return newTail - 1;
            });

            // step 3. add allocated addresses to each stream's address map
            // (to keep track of all updates to this stream)
            streamsAddressMap.compute(uuid, (streamId, addressMap) -> {
                if (addressMap == null) {
                    addressMap = new StreamAddressSpace();
                }

                for (long i = globalLogTail; i < newTail; i++) {
                    addressMap.addAddress(i);
                }
                return addressMap;
            });
        }

        // update the cache of conflict parameters
        if (tokenRequest.hasTxnResolution()) {
            tokenRequest.getTxnResolution().getWriteConflictParamsSetList()
                    .forEach((item) -> {
                        // insert an entry with the new timestamp using the
                        // hash code based on the param and the stream id.
                        item.getValueList().forEach(conflictParam ->
                                cache.put(new ConflictTxStream(getUUID(item.getKey()),
                                        conflictParam.toByteArray(), newTail - 1)));
                    });
        }
        if (log.isTraceEnabled()) {
            log.trace("handleAllocation: token={} backpointers={}",
                    globalLogTail, backPointerMap.build());
        }
        // return the token response with the global tail and the streams backpointers
        Token newToken = new Token(sequencerEpoch, globalLogTail);
        globalLogTail = newTail;

        // Note: we reuse the request header as the ignore_cluster_id and
        // ignore_epoch fields are the same in both cases.
        ResponseMsg response = getResponseMsg(
                getHeaderMsg(req.getHeader()), getTokenResponseMsg(newToken, backPointerMap.build()));
        r.sendResponse(response, ctx);
    }

    /**
     * This method handles the request of streams addresses.
     * <p>
     * The request of address spaces can be of two types:
     * - For specific streams (and specific ranges for each stream).
     * - For all streams (complete range).
     * <p>
     * The response contains the requested streams address maps and the global log tail.
     */
    @RequestHandler(type = PayloadCase.STREAMS_ADDRESS_REQUEST)
    private void handleStreamsAddressRequest(@Nonnull RequestMsg req,
                                             @Nonnull ChannelHandlerContext ctx,
                                             @Nonnull IServerRouter r) {
        StreamsAddressRequestMsg streamsAddressRequest =
                req.getPayload().getStreamsAddressRequest();
        Map<UUID, StreamAddressSpace> respStreamsAddressMap;
        StreamsAddressRequestMsg.Type reqType = streamsAddressRequest.getReqType();

        if (reqType == StreamsAddressRequestMsg.Type.STREAMS) {
            respStreamsAddressMap =
                    getStreamsAddressesMap(streamsAddressRequest.getStreamRangeList());
        } else if (reqType == StreamsAddressRequestMsg.Type.ALL_STREAMS) {
            // Retrieve address space for all streams
            respStreamsAddressMap = new HashMap<>(streamsAddressMap);
        } else {
            throw new IllegalArgumentException("handleStreamsAddressRequest: " +
                    "Received an " + reqType + "type of streamsAddressRequestMsg.");
        }
        if (log.isTraceEnabled()) {
            log.trace("handleStreamsAddressRequest: return address space for streams [{}]",
                    respStreamsAddressMap.keySet());
        }
        StreamsAddressResponse streamsAddressResponse =
                new StreamsAddressResponse(getGlobalLogTail(), respStreamsAddressMap);

        // Note: we reuse the request header as the ignore_cluster_id and
        // ignore_epoch fields are the same in both cases.
        ResponseMsg response = getResponseMsg(
                getHeaderMsg(req.getHeader()),
                getStreamsAddressResponseMsg(
                        streamsAddressResponse.getLogTail(),
                        streamsAddressResponse.getEpoch(),
                        streamsAddressResponse.getAddressMap()
                )
        );

        r.sendResponse(response, ctx);
    }

    /**
     * Return the address space for each stream in the requested ranges.
     *
     * @param addressRanges list of requested a stream and ranges.
     * @return map of stream to address space.
     */
    private Map<UUID, StreamAddressSpace> getStreamsAddressesMap(
            List<StreamAddressRangeMsg> addressRanges) {
        Map<UUID, StreamAddressSpace> requestedAddressSpaces = new HashMap<>();

        for (StreamAddressRangeMsg streamAddressRange : addressRanges) {
            UUID streamId = getUUID(streamAddressRange.getStreamId());
            // Get all addresses in the requested range
            if (streamsAddressMap.containsKey(streamId)) {
                StreamAddressSpace addressesInRange = streamsAddressMap.get(streamId)
                        .getAddressesInRange(getStreamAddressRange(streamAddressRange));
                requestedAddressSpaces.put(streamId, addressesInRange);
            } else {
                requestedAddressSpaces.put(streamId, new StreamAddressSpace(Address.NON_EXIST, Collections.EMPTY_SET));
            }
        }

        return requestedAddressSpaces;
    }


    /**
     * Used by the unit tests to inject a custom value for the required parameters through the
     * constructor
     *
     * The default implementations are listed here which will be overloaded with
     * the custom return values in unit tests.
     *
     * Note: This class should always return the default/initial objects and should not have
     *       any logic as it will never be tested.
     */
    static class SequencerServerInitializer {
        Map<UUID, StreamAddressSpace> getStreamAddressSpaceMap() {
            return new HashMap<>();
        }

        Map<UUID, Long> getStreamTailToGlobalTailMap() {
            return new HashMap<>();
        }

        SequencerServerCache getSequencerServerCache(int cacheSize, long maxConflictNewSequencer) {
            return new SequencerServerCache(cacheSize, maxConflictNewSequencer);
        }

        Long getGlobalLogTail() {
            return Address.getMinAddress();
        }
    }
}
