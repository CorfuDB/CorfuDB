package org.corfudb.protocols.service;

import com.google.common.collect.EnumBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.proto.Common.UuidToLongPairMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;
import org.corfudb.runtime.proto.Common.UuidToStreamAddressSpacePairMsg;
import org.corfudb.runtime.proto.service.Sequencer.BootstrapSequencerRequestMsg;
import org.corfudb.runtime.proto.service.Sequencer.BootstrapSequencerResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.SequencerMetricsRequestMsg;
import org.corfudb.runtime.proto.service.Sequencer.SequencerMetricsResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.SequencerTrimRequestMsg;
import org.corfudb.runtime.proto.service.Sequencer.SequencerTrimResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.StreamsAddressRequestMsg;
import org.corfudb.runtime.proto.service.Sequencer.StreamsAddressResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.TokenRequestMsg;
import org.corfudb.runtime.proto.service.Sequencer.TokenRequestMsg.TokenRequestType;
import org.corfudb.runtime.proto.service.Sequencer.TokenResponseMsg;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.protocols.CorfuProtocolCommon.*;
import static org.corfudb.protocols.CorfuProtocolTxResolution.*;

@Slf4j
public class CorfuProtocolSequencer {
    private static final EnumBiMap<TokenType, TokenResponseMsg.TokenType> tokenResponseTypeMap =
            EnumBiMap.create(ImmutableMap.of(
                    TokenType.NORMAL, TokenResponseMsg.TokenType.TX_NORMAL,
                    TokenType.TX_ABORT_CONFLICT, TokenResponseMsg.TokenType.TX_ABORT_CONFLICT,
                    TokenType.TX_ABORT_NEWSEQ, TokenResponseMsg.TokenType.TX_ABORT_NEWSEQ,
                    TokenType.TX_ABORT_SEQ_OVERFLOW, TokenResponseMsg.TokenType.TX_ABORT_SEQ_OVERFLOW,
                    TokenType.TX_ABORT_SEQ_TRIM, TokenResponseMsg.TokenType.TX_ABORT_SEQ_TRIM)
            );

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

    public static RequestPayloadMsg getTokenRequestMsg(long numTokens, List<UUID> streams) {
        TokenRequestMsg.Builder tokenRequestBuilder = TokenRequestMsg.newBuilder();
        tokenRequestBuilder.setNumTokens(numTokens);

        if(numTokens == 0) {
            tokenRequestBuilder.setRequestType(TokenRequestType.TK_QUERY);
            tokenRequestBuilder.addAllStreams(streams.stream()
                    .map(CorfuProtocolCommon::getUuidMsg).collect(Collectors.toList()));
        } else if(streams == null || streams.isEmpty()) {
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

    public static ResponsePayloadMsg getTokenResponseMsg(TokenType type, byte[] conflictKey,
                                                         UUID conflictStream, Token token,
                                                         Map<UUID, Long> backpointerMap, Map<UUID, Long> streamTails) {
        return ResponsePayloadMsg.newBuilder()
                .setTokenResponse(TokenResponseMsg.newBuilder()
                        .setRespType(tokenResponseTypeMap.get(type))
                        .setConflictKey(ByteString.copyFrom(conflictKey))
                        .setConflictStream(getUuidMsg(conflictStream))
                        .setToken(getTokenMsg(token))
                        .addAllBackpointerMap(backpointerMap.entrySet()
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

    public static ResponsePayloadMsg getTokenResponseMsg(Token token, Map<UUID, Long> backpointerMap) {
        return getTokenResponseMsg(TokenType.NORMAL, TokenResponse.NO_CONFLICT_KEY,
                TokenResponse.NO_CONFLICT_STREAM, token, backpointerMap, Collections.emptyMap());
    }

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

    public static RequestPayloadMsg getSequencerMetricsRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setSequencerMetricsRequest(SequencerMetricsRequestMsg.getDefaultInstance())
                .build();
    }

    public static ResponsePayloadMsg getSequencerMetricsResponseMsg(SequencerMetrics sequencerMetrics) {
        return ResponsePayloadMsg.newBuilder()
                .setSequencerMetricsResponse(SequencerMetricsResponseMsg.newBuilder()
                        .setSequencerMetrics(getSequencerMetricsMsg(sequencerMetrics))
                        .build())
                .build();
    }

    public static RequestPayloadMsg getSequencerTrimRequestMsg(long trimMark) {
        return RequestPayloadMsg.newBuilder()
                .setSequencerTrimRequest(SequencerTrimRequestMsg.newBuilder()
                        .setTrimMark(trimMark)
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getSequencerTrimResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setSequencerTrimResponse(SequencerTrimResponseMsg.getDefaultInstance())
                .build();
    }

    public static RequestPayloadMsg getStreamsAddressRequestMsg(List<StreamAddressRange> streamsAddressesRange) {
        return RequestPayloadMsg.newBuilder()
                .setStreamsAddressRequest(StreamsAddressRequestMsg.newBuilder()
                        .setReqType(StreamsAddressRequestMsg.Type.STREAMS)
                        .addAllStreamRange(streamsAddressesRange.stream()
                                .map(CorfuProtocolCommon::getStreamAddressRangeMsg)
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    public static RequestPayloadMsg getStreamsAddressRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setStreamsAddressRequest(StreamsAddressRequestMsg.newBuilder()
                        .setReqType(StreamsAddressRequestMsg.Type.ALL_STREAMS)
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getStreamsAddressResponseMsg(long logTail, Map<UUID, StreamAddressSpace> addressMap) {
        return ResponsePayloadMsg.newBuilder()
                .setStreamsAddressResponse(StreamsAddressResponseMsg.newBuilder()
                        .setLogTail(logTail)
                        .addAllAddressMap(addressMap.entrySet()
                                .stream()
                                .map(entry -> UuidToStreamAddressSpacePairMsg.newBuilder()
                                        .setKey(getUuidMsg(entry.getKey()))
                                        .setValue(getStreamAddressSpaceMsg(entry.getValue()))
                                        .build())
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    public static StreamsAddressResponse getStreamsAddressResponse(StreamsAddressResponseMsg msg) {
        return new StreamsAddressResponse(msg.getLogTail(),
                msg.getAddressMapList()
                        .stream()
                        .collect(Collectors.<UuidToStreamAddressSpacePairMsg, UUID, StreamAddressSpace>toMap(
                                entry -> getUUID(entry.getKey()),
                                entry -> getStreamAddressSpace(entry.getValue()))));
    }

    public static RequestPayloadMsg getBootstrapSequencerRequestMsg(Map<UUID, StreamAddressSpace> streamAddressSpaceMap,
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
                                        .setKey(getUuidMsg(entry.getKey()))
                                        .setValue(getStreamAddressSpaceMsg(entry.getValue()))
                                        .build())
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getBootstrapSequencerResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setBootstrapSequencerResponse(BootstrapSequencerResponseMsg.getDefaultInstance())
                .build();
    }
}
