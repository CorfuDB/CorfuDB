package org.corfudb.protocols.service;

import com.google.common.collect.EnumBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TokenType;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.proto.RpcCommon.UuidToLongPairMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidToStreamAddressSpacePairMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;
import org.corfudb.runtime.proto.service.Sequencer.BootstrapSequencerRequestMsg;
import org.corfudb.runtime.proto.service.Sequencer.BootstrapSequencerResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.SequencerMetricsRequestMsg;
import org.corfudb.runtime.proto.service.Sequencer.SequencerMetricsResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.SequencerTrimRequestMsg;
import org.corfudb.runtime.proto.service.Sequencer.SequencerTrimResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.StreamsAddressRequestMsg;
import org.corfudb.runtime.proto.service.Sequencer.TokenRequestMsg;
import org.corfudb.runtime.proto.service.Sequencer.TokenRequestMsg.TokenRequestType;
import org.corfudb.runtime.proto.service.Sequencer.TokenResponseMsg;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.protocols.CorfuProtocolCommon.getSequencerMetricsMsg;
import static org.corfudb.protocols.CorfuProtocolCommon.getStreamAddressSpaceMsg;
import static org.corfudb.protocols.CorfuProtocolCommon.getTokenMsg;
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.CorfuProtocolTxResolution.getTxResolutionInfoMsg;

/**
 * This class provides methods for creating the Protobuf objects defined in sequencer.proto.
 * These provide the interface for obtaining the Protobuf and the Java representations of the
 * following RequestMsg RPCs and their ResponseMsg counterparts -
 * - TokenRequestMsg
 * - BootstrapSequencerRequestMsg
 * - SequencerTrimRequestMsg
 * - SequencerMetricsRequestMsg
 * - StreamsAddressRequestMsg
 * These methods are used by both the client and the server.
 */
@Slf4j
public final class CorfuProtocolSequencer {
    // It stores the EnumBiMap of the Java and Protobuf TokenTypes for efficient conversions.
    private static final EnumBiMap<TokenType, TokenResponseMsg.TokenType> tokenResponseTypeMap =
            EnumBiMap.create(ImmutableMap.of(
                    TokenType.NORMAL, TokenResponseMsg.TokenType.TX_NORMAL,
                    TokenType.TX_ABORT_CONFLICT, TokenResponseMsg.TokenType.TX_ABORT_CONFLICT,
                    TokenType.TX_ABORT_NEWSEQ, TokenResponseMsg.TokenType.TX_ABORT_NEWSEQ,

                    TokenType.TX_ABORT_SEQ_OVERFLOW,
                    TokenResponseMsg.TokenType.TX_ABORT_SEQ_OVERFLOW,

                    TokenType.TX_ABORT_SEQ_TRIM, TokenResponseMsg.TokenType.TX_ABORT_SEQ_TRIM)
            );

    /**
     * Returns the Protobuf {@link RequestPayloadMsg} with the {@link TokenRequestMsg} payload
     * constructed from the given parameters.
     *
     * @param numTokens    the number of tokens
     * @param streams      a list of streams of Java UUID type
     * @param conflictInfo a {@link TxResolutionInfo} object
     * @return the Protobuf {@link RequestPayloadMsg}
     */
    public static RequestPayloadMsg getTokenRequestMsg(long numTokens, List<UUID> streams,
                                                       TxResolutionInfo conflictInfo) {
        return RequestPayloadMsg.newBuilder()
                .setTokenRequest(TokenRequestMsg.newBuilder()
                        .setRequestType(TokenRequestType.TK_TX)
                        .setNumTokens(numTokens)
                        .setTxnResolution(getTxResolutionInfoMsg(conflictInfo))
                        .addAllStreams(streams.stream()
                                .map(CorfuProtocolCommon::getUuidMsg)
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    /**
     * Returns the Protobuf {@link RequestPayloadMsg} with the {@link TokenRequestMsg} payload
     * constructed from the given parameters.
     * <p>
     * The type of the {@link TokenRequestType} depends on the parameters passed.
     * - If numTokens is 0, then the type is TK_QUERY.
     * - Else if steams is null or empty, then the type is TK_RAW.
     * - Otherwise, the type is TK_MULTI_STREAM.
     *
     * @param numTokens the number of tokens
     * @param streams   a list of streams to be included in the {@link TokenRequestMsg}
     * @return the Protobuf {@link RequestPayloadMsg}
     */
    public static RequestPayloadMsg getTokenRequestMsg(long numTokens, List<UUID> streams) {
        TokenRequestMsg.Builder tokenRequestBuilder = TokenRequestMsg.newBuilder();
        tokenRequestBuilder.setNumTokens(numTokens);

        if (numTokens == 0) {
            tokenRequestBuilder.setRequestType(TokenRequestType.TK_QUERY);
            tokenRequestBuilder.addAllStreams(streams.stream()
                    .map(CorfuProtocolCommon::getUuidMsg).collect(Collectors.toList()));
        } else if (streams == null || streams.isEmpty()) {
            tokenRequestBuilder.setRequestType(TokenRequestType.TK_RAW);
        } else {
            tokenRequestBuilder.setRequestType(TokenRequestType.TK_MULTI_STREAM);
            tokenRequestBuilder.addAllStreams(streams.stream()
                    .map(CorfuProtocolCommon::getUuidMsg).collect(Collectors.toList()));
        }

        return RequestPayloadMsg.newBuilder()
                .setTokenRequest(tokenRequestBuilder.build())
                .build();
    }

    /**
     * Returns the Protobuf {@link ResponsePayloadMsg} object with the {@link TokenResponseMsg} payload
     * constructed from the given parameters.
     *
     * @param type           the {@link TokenType} of the response object.
     * @param conflictKey    an array of bytes representing the conflictKey
     * @param conflictStream the Java UUID representing the conflictStream
     * @param token          the response token
     * @param backPointerMap the backPointerMap of UUID to Long type
     * @param streamTails    the streamsTails of UUID to Long type
     * @return the Protobuf {@link ResponsePayloadMsg} object
     */
    public static ResponsePayloadMsg getTokenResponseMsg(TokenType type,
                                                         byte[] conflictKey,
                                                         UUID conflictStream,
                                                         Token token,
                                                         Map<UUID, Long> backPointerMap,
                                                         Map<UUID, Long> streamTails) {
        return ResponsePayloadMsg.newBuilder()
                .setTokenResponse(TokenResponseMsg.newBuilder()
                        .setRespType(tokenResponseTypeMap.get(type))
                        .setConflictKey(ByteString.copyFrom(conflictKey))
                        .setConflictStream(getUuidMsg(conflictStream))
                        .setToken(getTokenMsg(token))
                        .addAllBackpointerMap(backPointerMap.entrySet()
                                .stream()
                                .map(e -> UuidToLongPairMsg.newBuilder()
                                        .setKey(getUuidMsg(e.getKey()))
                                        .setValue(e.getValue())
                                        .build())
                                .collect(Collectors.toList()))
                        .addAllStreamTails(streamTails.entrySet()
                                .stream()
                                .map(e -> UuidToLongPairMsg.newBuilder()
                                        .setKey(getUuidMsg(e.getKey()))
                                        .setValue(e.getValue())
                                        .build())
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    /**
     * Returns the Protobuf {@link ResponsePayloadMsg} object with the {@link TokenResponseMsg} payload
     * constructed from the given parameters.
     *
     * @param token          the response token
     * @param backPointerMap the backPointerMap of UUID to Long type
     * @return the Protobuf {@link ResponsePayloadMsg} object
     */
    public static ResponsePayloadMsg getTokenResponseMsg(Token token,
                                                         Map<UUID, Long> backPointerMap) {
        return getTokenResponseMsg(TokenType.NORMAL, TokenResponse.NO_CONFLICT_KEY,
                TokenResponse.NO_CONFLICT_STREAM, token, backPointerMap, Collections.emptyMap());
    }

    /**
     * Returns the Java {@link TokenResponse} object from the given {link TokenResponseMsg}
     * Protobuf object.
     *
     * @param msg the {@link TokenResponseMsg} Protobuf object
     * @return the {@link TokenResponse} object
     */
    public static TokenResponse getTokenResponse(TokenResponseMsg msg) {
        return new TokenResponse(
                tokenResponseTypeMap.inverse().get(msg.getRespType()),
                msg.getConflictKey().toByteArray(),
                getUUID(msg.getConflictStream()),
                new Token(msg.getToken().getEpoch(), msg.getToken().getSequence()),
                msg.getBackpointerMapList().stream().collect(Collectors.<UuidToLongPairMsg, UUID, Long>toMap(
                        entry -> getUUID(entry.getKey()), UuidToLongPairMsg::getValue)),
                msg.getStreamTailsList().stream().collect(Collectors.<UuidToLongPairMsg, UUID, Long>toMap(
                        entry -> getUUID(entry.getKey()), UuidToLongPairMsg::getValue))
        );
    }

    /**
     * Returns a new {@link RequestPayloadMsg} Protobuf object consisting of a default
     * {@link SequencerMetricsRequestMsg} object.
     *
     * @return the {@link RequestPayloadMsg} Protobuf object
     */
    public static RequestPayloadMsg getDefaultSequencerMetricsRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setSequencerMetricsRequest(SequencerMetricsRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns the {@link RequestPayloadMsg} Protobuf object consisting of a
     * {@link SequencerMetricsResponseMsg} object with sequencerMetrics set from the parameter.
     *
     * @param sequencerMetrics the sequencer metrics object to be set in the
     *                         {@link SequencerMetricsResponseMsg} object
     * @return a new {@link RequestPayloadMsg} Protobuf object
     */
    public static ResponsePayloadMsg getSequencerMetricsResponseMsg(
            SequencerMetrics sequencerMetrics) {
        return ResponsePayloadMsg.newBuilder()
                .setSequencerMetricsResponse(SequencerMetricsResponseMsg.newBuilder()
                        .setSequencerMetrics(getSequencerMetricsMsg(sequencerMetrics))
                        .build())
                .build();
    }

    /**
     * Returns a new {@link RequestPayloadMsg} Protobuf object consisting of a
     * {@link SequencerTrimRequestMsg} object with trimMark set from the parameter.
     *
     * @param trimMark the trimMark required on the SequencerTrimRequestMsg.
     * @return a new {@link RequestPayloadMsg} Protobuf object
     */
    public static RequestPayloadMsg getSequencerTrimRequestMsg(long trimMark) {
        return RequestPayloadMsg.newBuilder()
                .setSequencerTrimRequest(SequencerTrimRequestMsg.newBuilder()
                        .setTrimMark(trimMark)
                        .build())
                .build();
    }

    /**
     * Returns a new {@link ResponsePayloadMsg} Protobuf object consisting of a default
     * {@link SequencerTrimResponseMsg} object.
     *
     * @return the {@link ResponsePayloadMsg} Protobuf object
     */
    public static ResponsePayloadMsg getSequencerTrimResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setSequencerTrimResponse(SequencerTrimResponseMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a new {@link RequestPayloadMsg} Protobuf object consisting of a
     * {@link StreamsAddressRequestMsg} object with trimMark set from the parameter.
     * Since the streams are set specifically, the request type of
     * {@link StreamsAddressRequestMsg} is STREAMS (instead of ALL_STREAMS or INVALID).
     *
     * @param streamsAddressesRange the streamsAddressesRange required on the
     *                              StreamsAddressRequestMsg.
     * @return a new {@link RequestPayloadMsg} Protobuf object
     */
    public static RequestPayloadMsg getStreamsAddressRequestMsg(
            List<StreamAddressRange> streamsAddressesRange) {
        return RequestPayloadMsg.newBuilder()
                .setStreamsAddressRequest(StreamsAddressRequestMsg.newBuilder()
                        .setReqType(StreamsAddressRequestMsg.Type.STREAMS)
                        .addAllStreamRange(streamsAddressesRange.stream()
                                .map(CorfuProtocolCommon::getStreamAddressRangeMsg)
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    /**
     * Returns a new {@link RequestPayloadMsg} Protobuf object consisting of a
     * {@link StreamsAddressRequestMsg} object.
     * Since the streams are not set specifically, the request type of
     * {@link StreamsAddressRequestMsg} is ALL_STREAMS (instead of STREAMS or INVALID).
     *
     * @return a new {@link RequestPayloadMsg} Protobuf object
     */
    public static RequestPayloadMsg getAllStreamsAddressRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setStreamsAddressRequest(StreamsAddressRequestMsg.newBuilder()
                        .setReqType(StreamsAddressRequestMsg.Type.ALL_STREAMS)
                        .build())
                .build();
    }

    /**
     * Returns the {@link RequestPayloadMsg} Java object from the
     * {@link BootstrapSequencerRequestMsg} Protobuf object.
     *
     * @param streamAddressSpaceMap       the addressMap of the streams to bootstrap the sequencer with
     * @param globalTail                  the globalTail of the request
     * @param sequencerEpoch              the sequencerEpoch of the request
     * @param bootstrapWithoutTailsUpdate the boolean value indicating to bootstrap with or without
     *                                    tails update.
     * @return a new {@link RequestPayloadMsg} Protobuf object
     */
    public static RequestPayloadMsg getBootstrapSequencerRequestMsg(
            Map<UUID, StreamAddressSpace> streamAddressSpaceMap,
            long globalTail, long sequencerEpoch,
            boolean bootstrapWithoutTailsUpdate) {
        return RequestPayloadMsg.newBuilder()
                .setBootstrapSequencerRequest(BootstrapSequencerRequestMsg.newBuilder()
                        .setGlobalTail(globalTail)
                        .setSequencerEpoch(sequencerEpoch)
                        .setBootstrapWithoutTailsUpdate(bootstrapWithoutTailsUpdate)
                        .addAllStreamsAddressMap(streamAddressSpaceMap.entrySet()
                                .stream()
                                .map(entry -> UuidToStreamAddressSpacePairMsg.newBuilder()
                                        .setStreamUuid(getUuidMsg(entry.getKey()))
                                        .setAddressSpace(getStreamAddressSpaceMsg(entry.getValue()))
                                        .build())
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    /**
     * Returns the {@link ResponsePayloadMsg} Java object from the
     * {@link BootstrapSequencerResponseMsg} Protobuf object.
     *
     * @param isBootstrapped the boolean value indicating if the server was bootstrapped or not
     * @return a new {@link ResponsePayloadMsg} Protobuf object
     */
    public static ResponsePayloadMsg getBootstrapSequencerResponseMsg(
            boolean isBootstrapped) {
        return ResponsePayloadMsg.newBuilder()
                .setBootstrapSequencerResponse(BootstrapSequencerResponseMsg.newBuilder()
                        .setIsBootstrapped(isBootstrapped)
                        .build())
                .build();
    }
}
