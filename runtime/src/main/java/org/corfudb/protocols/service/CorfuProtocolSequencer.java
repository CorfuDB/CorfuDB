package org.corfudb.protocols.service;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.protocols.CorfuProtocolCommon.*;
import static org.corfudb.protocols.CorfuProtocolTxResolution.*;

@Slf4j
public class CorfuProtocolSequencer {
    private static RequestPayloadMsg getTokenRequestMsg(long numTokens, List<UUID> streams,
                                                       TxResolutionInfo conflictInfo, TokenRequestType type) {
        return RequestPayloadMsg.newBuilder()
                .setTokenRequest(TokenRequestMsg.newBuilder()
                        .setRequestType(type)
                        .setNumTokens(numTokens)
                        .setTxnResolution(getTxResolutionInfoMsg(conflictInfo))
                        .addAllStreams(streams.stream()
                                .map(CorfuProtocolCommon::getUuidMsg)
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    public static RequestPayloadMsg getTokenRequestMsg(long numTokens, List<UUID> streams,
                                                       TxResolutionInfo conflictInfo) {
        return getTokenRequestMsg(numTokens, streams, conflictInfo, TokenRequestType.TK_TX);
    }

    /* public static RequestPayloadMsg getTokenRequestMsg(long numTokens, List<UUID> streams) {

    } */

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
                                .map(entry -> {
                                    return UuidToStreamAddressSpacePairMsg.newBuilder()
                                            .setKey(getUuidMsg(entry.getKey()))
                                            .setValue(getStreamAddressSpaceMsg(entry.getValue()))
                                            .build();
                                })
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    public static StreamsAddressResponse getStreamsAddressResponse(StreamsAddressResponseMsg msg) {
        return new StreamsAddressResponse(msg.getLogTail(),
                msg.getAddressMapList()
                        .stream()
                        .collect(Collectors.<UuidToStreamAddressSpacePairMsg, UUID, StreamAddressSpace>toMap(
                                entry -> { return getUUID(entry.getKey()); },
                                entry -> { return getStreamAddressSpace(entry.getValue()); })));
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
                                .map(entry -> {
                                    return UuidToStreamAddressSpacePairMsg.newBuilder()
                                            .setKey(getUuidMsg(entry.getKey()))
                                            .setValue(getStreamAddressSpaceMsg(entry.getValue()))
                                            .build();
                                })
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
