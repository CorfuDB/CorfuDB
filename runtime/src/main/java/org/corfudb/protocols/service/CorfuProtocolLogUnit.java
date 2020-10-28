package org.corfudb.protocols.service;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.CorfuProtocolLogData;
import org.corfudb.protocols.wireprotocol.InspectAddressesResponse;
import org.corfudb.protocols.wireprotocol.KnownAddressResponse;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.proto.Common.UuidToLongPairMsg;
import org.corfudb.runtime.proto.Common.UuidToStreamAddressSpacePairMsg;
import org.corfudb.runtime.proto.ServerErrors.ValueAdoptedErrorMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;
import org.corfudb.runtime.proto.service.LogUnit.CompactRequestMsg;
import org.corfudb.runtime.proto.service.LogUnit.CompactResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.CommittedTailRequestMsg;
import org.corfudb.runtime.proto.service.LogUnit.CommittedTailResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.FlushCacheRequestMsg;
import org.corfudb.runtime.proto.service.LogUnit.FlushCacheResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.InspectAddressesRequestMsg;
import org.corfudb.runtime.proto.service.LogUnit.InspectAddressesResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.KnownAddressRequestMsg;
import org.corfudb.runtime.proto.service.LogUnit.KnownAddressResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.LogAddressSpaceRequestMsg;
import org.corfudb.runtime.proto.service.LogUnit.LogAddressSpaceResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.RangeWriteLogRequestMsg;
import org.corfudb.runtime.proto.service.LogUnit.RangeWriteLogResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.ReadLogRequestMsg;
import org.corfudb.runtime.proto.service.LogUnit.ReadLogResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.ResetLogUnitRequestMsg;
import org.corfudb.runtime.proto.service.LogUnit.ResetLogUnitResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.TailRequestMsg;
import org.corfudb.runtime.proto.service.LogUnit.TailResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.TrimLogRequestMsg;
import org.corfudb.runtime.proto.service.LogUnit.TrimLogResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.TrimMarkRequestMsg;
import org.corfudb.runtime.proto.service.LogUnit.TrimMarkResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.UpdateCommittedTailRequestMsg;
import org.corfudb.runtime.proto.service.LogUnit.UpdateCommittedTailResponseMsg;
import org.corfudb.runtime.proto.service.LogUnit.WriteLogRequestMsg;
import org.corfudb.runtime.proto.service.LogUnit.WriteLogResponseMsg;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import java.util.*;
import java.util.stream.Collectors;

import static org.corfudb.protocols.CorfuProtocolCommon.*;
import static org.corfudb.protocols.CorfuProtocolLogData.*;

@Slf4j
public class CorfuProtocolLogUnit {
    public static RequestPayloadMsg getReadLogRequestMsg(List<Long> addresses, boolean cacheable) {
        return RequestPayloadMsg.newBuilder()
                .setReadLogRequest(ReadLogRequestMsg.newBuilder()
                        .setCacheResults(cacheable)
                        .addAllAddress(addresses)
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getReadLogResponseMsg(Map<Long, LogData> addresses) {
        return ResponsePayloadMsg.newBuilder()
                .setReadLogResponse(ReadLogResponseMsg.newBuilder()
                        .addAllResponse(addresses.entrySet()
                                .stream()
                                .map(e -> {
                                    return getReadResponseMsg(e.getKey(), e.getValue());
                                })
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    public static ReadResponse getReadResponse(ReadLogResponseMsg msg) {
        ReadResponse rr = new ReadResponse();
        msg.getResponseList().forEach(e -> {
            rr.put(e.getAddress(), getLogData(e.getLogData()));
        });

        return rr;
    }

    public static ReadResponse getReadResponse(ValueAdoptedErrorMsg msg) {
        ReadResponse rr = new ReadResponse();
        msg.getResponseList().forEach(e -> {
            rr.put(e.getAddress(), getLogData(e.getLogData()));
        });

        return rr;
    }

    public static RequestPayloadMsg getWriteLogRequestMsg(LogData data) {
        return RequestPayloadMsg.newBuilder()
                .setWriteLogRequest(WriteLogRequestMsg.newBuilder()
                        .setLogData(getLogDataMsg(data))
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getWriteLogResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setWriteLogResponse(WriteLogResponseMsg.getDefaultInstance())
                .build();
    }

    public static RequestPayloadMsg getRangeWriteLogRequestMsg(List<LogData> range) {
        return RequestPayloadMsg.newBuilder()
                .setRangeWriteLogRequest(RangeWriteLogRequestMsg.newBuilder()
                        .addAllLogData(range.stream()
                                .map(CorfuProtocolLogData::getLogDataMsg)
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getRangeWriteLogResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setRangeWriteLogResponse(RangeWriteLogResponseMsg.getDefaultInstance())
                .build();
    }

    public static RequestPayloadMsg getInspectAddressesRequestMsg(List<Long> addresses) {
        return RequestPayloadMsg.newBuilder()
                .setInspectAddressesRequest(InspectAddressesRequestMsg.newBuilder()
                        .addAllAddress(addresses)
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getInspectAddressesResponseMsg(List<Long> emptyAddresses) {
        return ResponsePayloadMsg.newBuilder()
                .setInspectAddressesResponse(InspectAddressesResponseMsg.newBuilder()
                        .addAllEmptyAddress(emptyAddresses)
                        .build())
                .build();
    }

    public static RequestPayloadMsg getTrimLogRequestMsg(Token address) {
        return RequestPayloadMsg.newBuilder()
                .setTrimLogRequest(TrimLogRequestMsg.newBuilder()
                        .setAddress(getTokenMsg(address))
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getTrimLogResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setTrimLogResponse(TrimLogResponseMsg.getDefaultInstance())
                .build();
    }

    public static RequestPayloadMsg getTrimMarkRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setTrimMarkRequest(TrimMarkRequestMsg.getDefaultInstance())
                .build();
    }

    public static ResponsePayloadMsg getTrimMarkResponseMsg(long trimMark) {
        return ResponsePayloadMsg.newBuilder()
                .setTrimMarkResponse(TrimMarkResponseMsg.newBuilder()
                        .setTrimMark(trimMark)
                        .build())
                .build();
    }

    public static RequestPayloadMsg getTailRequestMsg(TailRequestMsg.Type type) {
        return RequestPayloadMsg.newBuilder()
                .setTailRequest(TailRequestMsg.newBuilder()
                        .setReqType(type)
                        .build())
                .build();
    }

    public static RequestPayloadMsg getTailRequestMsg(List<UUID> streamIds) {
        return RequestPayloadMsg.newBuilder()
                .setTailRequest(TailRequestMsg.newBuilder()
                        .setReqType(TailRequestMsg.Type.STREAMS_TAILS)
                        .addAllStream(streamIds.stream()
                                .map(CorfuProtocolCommon::getUuidMsg)
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getTailResponseMsg(long epoch, long logTail, Map<UUID, Long> streamTails) {
        return ResponsePayloadMsg.newBuilder()
                .setTailResponse(TailResponseMsg.newBuilder()
                        .setEpoch(epoch)
                        .setLogTail(logTail)
                        .addAllStreamTail(streamTails.entrySet()
                                .stream()
                                .map(e -> UuidToLongPairMsg.newBuilder()
                                        .setKey(getUuidMsg(e.getKey()))
                                        .setValue(e.getValue())
                                        .build())
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    public static TailsResponse getTailsResponse(TailResponseMsg msg) {
        return new TailsResponse(msg.getEpoch(), msg.getLogTail(),
                msg.getStreamTailList()
                        .stream()
                        .collect(Collectors.<UuidToLongPairMsg, UUID, Long>toMap(
                                e -> { return getUUID(e.getKey()); },
                                UuidToLongPairMsg::getValue)));
    }

    public static InspectAddressesResponse getInspectAddressesResponse(InspectAddressesResponseMsg msg) {
        InspectAddressesResponse ir = new InspectAddressesResponse();
        msg.getEmptyAddressList().forEach(ir::add);

        return ir;
    }

    public static RequestPayloadMsg getCompactRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setCompactRequest(CompactRequestMsg.getDefaultInstance())
                .build();
    }

    public static ResponsePayloadMsg getCompactResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setCompactResponse(CompactResponseMsg.getDefaultInstance())
                .build();
    }

    public static RequestPayloadMsg getFlushCacheRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setFlushCacheRequest(FlushCacheRequestMsg.getDefaultInstance())
                .build();
    }

    public static ResponsePayloadMsg getFlushCacheResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setFlushCacheResponse(FlushCacheResponseMsg.getDefaultInstance())
                .build();
    }

    public static RequestPayloadMsg getLogAddressSpaceRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setLogAddressSpaceRequest(LogAddressSpaceRequestMsg.getDefaultInstance())
                .build();
    }

    public static ResponsePayloadMsg getLogAddressSpaceResponseMsg(long tail, Map<UUID, StreamAddressSpace> addressMap) {
        return ResponsePayloadMsg.newBuilder()
                .setLogAddressSpaceResponse(
                        LogAddressSpaceResponseMsg.newBuilder()
                                .setLogTail(tail)
                                .addAllAddressMap(addressMap.entrySet()
                                        .stream()
                                        .map(e -> UuidToStreamAddressSpacePairMsg.newBuilder()
                                                .setKey(getUuidMsg(e.getKey()))
                                                .setValue(getStreamAddressSpaceMsg(e.getValue()))
                                                .build())
                                        .collect(Collectors.toList()))
                                .build())
                .build();
    }

    public static RequestPayloadMsg getKnownAddressRequestMsg(long startRange, long endRange) {
        return RequestPayloadMsg.newBuilder()
                .setKnownAddressRequest(KnownAddressRequestMsg.newBuilder()
                        .setStartRange(startRange)
                        .setEndRange(endRange)
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getKnownAddressResponseMsg(Set<Long> addresses) {
        return ResponsePayloadMsg.newBuilder()
                .setKnownAddressResponse(KnownAddressResponseMsg.newBuilder()
                        .addAllKnownAddress(addresses)
                        .build())
                .build();
    }

    public static KnownAddressResponse getKnownAddressResponse(KnownAddressResponseMsg msg) {
        return new KnownAddressResponse(new HashSet<>(msg.getKnownAddressList()));
    }

    public static RequestPayloadMsg getCommittedTailRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setCommittedTailRequest(CommittedTailRequestMsg.getDefaultInstance())
                .build();
    }

    public static ResponsePayloadMsg getCommittedTailResponseMsg(long committedTail) {
        return ResponsePayloadMsg.newBuilder()
                .setCommittedTailResponse(CommittedTailResponseMsg.newBuilder()
                        .setCommittedTail(committedTail)
                        .build())
                .build();
    }

    public static RequestPayloadMsg getUpdateCommittedTailRequestMsg(long committedTail) {
        return RequestPayloadMsg.newBuilder()
                .setUpdateCommittedTailRequest(
                        UpdateCommittedTailRequestMsg.newBuilder()
                                .setCommittedTail(committedTail)
                                .build())
                .build();
    }

    public static ResponsePayloadMsg getUpdateCommittedTailResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setUpdateCommittedTailResponse(UpdateCommittedTailResponseMsg.getDefaultInstance())
                .build();
    }

    public static RequestPayloadMsg getResetLogUnitRequestMsg(int epoch) {
        return RequestPayloadMsg.newBuilder()
                .setResetLogUnitRequest(ResetLogUnitRequestMsg.newBuilder()
                        .setEpoch(epoch)
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getResetLogUnitResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setResetLogUnitResponse(ResetLogUnitResponseMsg.getDefaultInstance())
                .build();
    }
}
