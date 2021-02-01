package org.corfudb.protocols.service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.CorfuProtocolLogData;
import org.corfudb.protocols.wireprotocol.InspectAddressesResponse;
import org.corfudb.protocols.wireprotocol.KnownAddressResponse;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.proto.RpcCommon.UuidToLongPairMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidToStreamAddressSpacePairMsg;
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

import static org.corfudb.protocols.CorfuProtocolCommon.getStreamAddressSpaceMsg;
import static org.corfudb.protocols.CorfuProtocolCommon.getTokenMsg;
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.CorfuProtocolLogData.getLogData;
import static org.corfudb.protocols.CorfuProtocolLogData.getLogDataMsg;
import static org.corfudb.protocols.CorfuProtocolLogData.getReadResponseMsg;

/**
 * This class provides methods for creating the Protobuf objects defined
 * in log_unit.proto. These provide the interface for performing the RPCs
 * handled by the LogUnit server.
 */
@Slf4j
public final class CorfuProtocolLogUnit {
    // Prevent class from being instantiated
    private CorfuProtocolLogUnit() {}

    /**
     * Returns a READ request that can be sent by the client.
     *
     * @param addresses  a list of addresses to read from
     * @param cacheable  true if the read result should be cached on the LogUnit server
     * @return           a RequestPayloadMsg containing the READ request
     */
    public static RequestPayloadMsg getReadLogRequestMsg(List<Long> addresses, boolean cacheable) {
        return RequestPayloadMsg.newBuilder()
                .setReadLogRequest(ReadLogRequestMsg.newBuilder()
                        .setCacheResults(cacheable)
                        .addAllAddress(addresses)
                        .build())
                .build();
    }

    /**
     * Returns a READ response that can be sent by the server.
     *
     * @param addresses  a map containing the log data from the addresses read
     * @return           a ResponsePayloadMsg containing the READ response
     */
    public static ResponsePayloadMsg getReadLogResponseMsg(Map<Long, LogData> addresses) {
        return ResponsePayloadMsg.newBuilder()
                .setReadLogResponse(ReadLogResponseMsg.newBuilder()
                        .addAllResponse(addresses.entrySet()
                                .stream()
                                .map(e -> getReadResponseMsg(e.getKey(), e.getValue()))
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    /**
     * Returns a ReadResponse from its Protobuf representation.
     *
     * @param msg  the desired Protobuf ReadLogResponse message
     * @return     an equivalent ReadResponse object
     */
    public static ReadResponse getReadResponse(ReadLogResponseMsg msg) {
        ReadResponse rr = new ReadResponse();
        msg.getResponseList().forEach(e -> rr.put(e.getAddress(), getLogData(e.getLogData())));

        return rr;
    }

    /**
     * Returns a WRITE request that can be sent by the client.
     *
     * @param data  the log data to write to the LogUnit
     * @return      a RequestPayloadMsg containing the WRITE request
     */
    public static RequestPayloadMsg getWriteLogRequestMsg(LogData data) {
        return RequestPayloadMsg.newBuilder()
                .setWriteLogRequest(WriteLogRequestMsg.newBuilder()
                        .setLogData(getLogDataMsg(data))
                        .build())
                .build();
    }

    /**
     * Returns a WRITE response that can be sent by the server.
     *
     * @return  a ResponsePayloadMsg containing the WRITE response
     */
    public static ResponsePayloadMsg getWriteLogResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setWriteLogResponse(WriteLogResponseMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a RANGE_WRITE request that can be sent by the client.
     *
     * @param range  the list of log data to write to the LogUnit
     * @return       a RequestPayloadMsg containing the RANGE_WRITE request
     */
    public static RequestPayloadMsg getRangeWriteLogRequestMsg(List<LogData> range) {
        return RequestPayloadMsg.newBuilder()
                .setRangeWriteLogRequest(RangeWriteLogRequestMsg.newBuilder()
                        .addAllLogData(range.stream()
                                .map(CorfuProtocolLogData::getLogDataMsg)
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    /**
     * Returns a RANGE_WRITE response that can be sent by the server.
     *
     * @return  a ResponsePayloadMsg containing the RANGE_WRITE response
     */
    public static ResponsePayloadMsg getRangeWriteLogResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setRangeWriteLogResponse(RangeWriteLogResponseMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns an INSPECT_ADDRESSES request that can be sent by the client.
     *
     * @param addresses  a list of global addresses to inspect
     * @return           a RequestPayloadMsg containing the INSPECT_ADDRESSES request
     */
    public static RequestPayloadMsg getInspectAddressesRequestMsg(List<Long> addresses) {
        return RequestPayloadMsg.newBuilder()
                .setInspectAddressesRequest(InspectAddressesRequestMsg.newBuilder()
                        .addAllAddress(addresses)
                        .build())
                .build();
    }

    /**
     * Returns an INSPECT_ADDRESSES response that can be sent by the server.
     *
     * @param emptyAddresses  the list of empty addresses
     * @return                a ResponsePayloadMsg containing the INSPECT_ADDRESSES response
     */
    public static ResponsePayloadMsg getInspectAddressesResponseMsg(List<Long> emptyAddresses) {
        return ResponsePayloadMsg.newBuilder()
                .setInspectAddressesResponse(InspectAddressesResponseMsg.newBuilder()
                        .addAllEmptyAddress(emptyAddresses)
                        .build())
                .build();
    }

    /**
     * Returns an InspectAddressesResponse from its Protobuf representation.
     *
     * @param msg  the desired Protobuf InspectAddressesResponse message
     * @return     an equivalent InspectAddressesResponse object
     */
    public static InspectAddressesResponse getInspectAddressesResponse(InspectAddressesResponseMsg msg) {
        return new InspectAddressesResponse(msg.getEmptyAddressList());
    }

    /**
     * Returns a TRIM_LOG (PREFIX_TRIM) request that can be sent by
     * the client.
     *
     * @param address  an address to trim up to (i.e. [0, address))
     * @return         a RequestPayloadMsg containing the TRIM_LOG request
     */
    public static RequestPayloadMsg getTrimLogRequestMsg(Token address) {
        return RequestPayloadMsg.newBuilder()
                .setTrimLogRequest(TrimLogRequestMsg.newBuilder()
                        .setAddress(getTokenMsg(address))
                        .build())
                .build();
    }

    /**
     * Returns a TRIM_LOG (PREFIX_TRIM) response that can be sent
     * by the server.
     *
     * @return  a ResponsePayloadMsg containing the TRIM_LOG response
     */
    public static ResponsePayloadMsg getTrimLogResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setTrimLogResponse(TrimLogResponseMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a TRIM_MARK request that can be sent by the client.
     *
     * @return  a RequestPayloadMsg containing the TRIM_MARK request
     */
    public static RequestPayloadMsg getTrimMarkRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setTrimMarkRequest(TrimMarkRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a TRIM_MARK response that can be sent by the server.
     *
     * @param trimMark  the first untrimmed address in the address space
     * @return          a ResponsePayloadMsg containing the TRIM_MARK response
     */
    public static ResponsePayloadMsg getTrimMarkResponseMsg(long trimMark) {
        return ResponsePayloadMsg.newBuilder()
                .setTrimMarkResponse(TrimMarkResponseMsg.newBuilder()
                        .setTrimMark(trimMark)
                        .build())
                .build();
    }

    /**
     * Returns a TAILS request of the specified type that can be sent by the client.
     *
     * @param type  the type of the TAILS request
     * @return      a RequestPayloadMsg containing the TAILS request
     */
    public static RequestPayloadMsg getTailRequestMsg(TailRequestMsg.Type type) {
        return RequestPayloadMsg.newBuilder()
                .setTailRequest(TailRequestMsg.newBuilder()
                        .setReqType(type)
                        .build())
                .build();
    }

    /**
     * Returns a TAILS response that can be sent by the server.
     *
     * @param epoch        the seal epoch
     * @param logTail      the global tail
     * @param streamTails  a map of stream specific tails
     * @return             a ResponsePayloadMsg containing the TAILS response
     */
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

    /**
     * Returns a TailsResponse from its Protobuf representation.
     *
     * @param msg  the desired Protobuf TailResponse message
     * @return     an equivalent TailsResponse object
     */
    public static TailsResponse getTailsResponse(TailResponseMsg msg) {
        return new TailsResponse(msg.getEpoch(), msg.getLogTail(),
                msg.getStreamTailList()
                        .stream()
                        .collect(Collectors.<UuidToLongPairMsg, UUID, Long>toMap(
                                e -> getUUID(e.getKey()),
                                UuidToLongPairMsg::getValue)));
    }

    /**
     * Returns a COMPACT request that can be sent by the client.
     *
     * @return  a RequestPayloadMsg containing the COMPACT request
     */
    public static RequestPayloadMsg getCompactRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setCompactRequest(CompactRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a COMPACT response that can be sent by the server.
     *
     * @return  a ResponsePayloadMsg containing the COMPACT response
     */
    public static ResponsePayloadMsg getCompactResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setCompactResponse(CompactResponseMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a FLUSH_CACHE request that can be sent by the client.
     *
     * @return  a RequestPayloadMsg containing the FLUSH_CACHE request
     */
    public static RequestPayloadMsg getFlushCacheRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setFlushCacheRequest(FlushCacheRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a FLUSH_CACHE response that can be sent by the server.
     *
     * @return  a ResponsePayloadMsg containing the FLUSH_CACHE response
     */
    public static ResponsePayloadMsg getFlushCacheResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setFlushCacheResponse(FlushCacheResponseMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a LOG_ADDRESS_SPACE request that can be sent by the client.
     *
     * @return  a RequestPayloadMsg containing the LOG_ADDRESS_SPACE request
     */
    public static RequestPayloadMsg getLogAddressSpaceRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setLogAddressSpaceRequest(LogAddressSpaceRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a LOG_ADDRESS_SPACE response that can be sent by the server.
     *
     * @param tail        the log's tail
     * @param epoch       the epoch the response is sealed with
     * @param addressMap  a per stream map with its corresponding address space
     * @return            a ResponsePayloadMsg containing the LOG_ADDRESS_SPACE response
     */
    public static ResponsePayloadMsg getLogAddressSpaceResponseMsg(long tail, long epoch,
                                                                   Map<UUID, StreamAddressSpace> addressMap) {
        return ResponsePayloadMsg.newBuilder()
                .setLogAddressSpaceResponse(
                        LogAddressSpaceResponseMsg.newBuilder()
                                .setLogTail(tail)
                                .setEpoch(epoch)
                                .addAllAddressMap(addressMap.entrySet()
                                        .stream()
                                        .map(e -> UuidToStreamAddressSpacePairMsg.newBuilder()
                                                .setStreamUuid(getUuidMsg(e.getKey()))
                                                .setAddressSpace(getStreamAddressSpaceMsg(e.getValue()))
                                                .build())
                                        .collect(Collectors.toList()))
                                .build())
                .build();
    }

    /**
     * Returns a KNOWN_ADDRESS request that can be sent by the client.
     *
     * @param startRange  the start of the range, inclusive
     * @param endRange    the end of the range, inclusive
     * @return            a RequestPayloadMsg containing the KNOWN_ADDRESS request
     */
    public static RequestPayloadMsg getKnownAddressRequestMsg(long startRange, long endRange) {
        return RequestPayloadMsg.newBuilder()
                .setKnownAddressRequest(KnownAddressRequestMsg.newBuilder()
                        .setStartRange(startRange)
                        .setEndRange(endRange)
                        .build())
                .build();
    }

    /**
     * Returns a KNOWN_ADDRESS response that can be sent by the server.
     *
     * @param addresses  the set of known addresses within the previously
     *                   specified range
     * @return           a ResponsePayloadMsg containing the KNOWN_ADDRESS response
     */
    public static ResponsePayloadMsg getKnownAddressResponseMsg(Set<Long> addresses) {
        return ResponsePayloadMsg.newBuilder()
                .setKnownAddressResponse(KnownAddressResponseMsg.newBuilder()
                        .addAllKnownAddress(addresses)
                        .build())
                .build();
    }

    /**
     * Returns a KnownAddressResponse from its Protobuf representation.
     *
     * @param msg  the desired Protobuf KnownAddressResponse message
     * @return     an equivalent KnownAddressResponse object
     */
    public static KnownAddressResponse getKnownAddressResponse(KnownAddressResponseMsg msg) {
        return new KnownAddressResponse(new HashSet<>(msg.getKnownAddressList()));
    }

    /**
     * Returns a COMMITTED_TAIL request that can be sent by the client.
     *
     * @return  a RequestPayloadMsg containing the COMMITTED_TAIL request
     */
    public static RequestPayloadMsg getCommittedTailRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setCommittedTailRequest(CommittedTailRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a COMMITTED_TAIL response that can be sent by the server.
     *
     * @param committedTail  the committed tail on this LogUnit
     * @return               a ResponsePayloadMsg containing the COMMITTED_TAIL response
     */
    public static ResponsePayloadMsg getCommittedTailResponseMsg(long committedTail) {
        return ResponsePayloadMsg.newBuilder()
                .setCommittedTailResponse(CommittedTailResponseMsg.newBuilder()
                        .setCommittedTail(committedTail)
                        .build())
                .build();
    }

    /**
     * Returns an UPDATE_COMMITTED_TAIL request that can be sent by the client.
     *
     * @param committedTail  the new committed tail to update
     * @return               a RequestPayloadMsg containing an UPDATE_COMMITTED_TAIL request
     */
    public static RequestPayloadMsg getUpdateCommittedTailRequestMsg(long committedTail) {
        return RequestPayloadMsg.newBuilder()
                .setUpdateCommittedTailRequest(
                        UpdateCommittedTailRequestMsg.newBuilder()
                                .setCommittedTail(committedTail)
                                .build())
                .build();
    }

    /**
     * Returns an UPDATE_COMMITTED_TAIL response that can be sent by the server.
     *
     * @return  a ResponsePayloadMsg containing the UPDATE_COMMITTED_TAIL response
     */
    public static ResponsePayloadMsg getUpdateCommittedTailResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setUpdateCommittedTailResponse(UpdateCommittedTailResponseMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a RESET_LOG_UNIT request that can be sent by the client.
     *
     * @param epoch  epoch to check and set epochWaterMark
     * @return       a RequestPayloadMsg containing the RESET_LOG_UNIT request
     */
    public static RequestPayloadMsg getResetLogUnitRequestMsg(long epoch) {
        return RequestPayloadMsg.newBuilder()
                .setResetLogUnitRequest(ResetLogUnitRequestMsg.newBuilder()
                        .setEpoch(epoch)
                        .build())
                .build();
    }

    /**
     * Returns a RESET_LOG_UNIT response that can be sent by the server.
     *
     * @return  a ResponsePayloadMsg containing the RESET_LOG_UNIT response
     */
    public static ResponsePayloadMsg getResetLogUnitResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setResetLogUnitResponse(ResetLogUnitResponseMsg.getDefaultInstance())
                .build();
    }
}
