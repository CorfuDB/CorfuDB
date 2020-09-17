package org.corfudb.protocols;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import lombok.NonNull;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.orchestrator.*;
import org.corfudb.runtime.protocol.proto.CorfuProtocol;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.*;
import org.corfudb.common.util.Address;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.OrchestratorResponse;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Request;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Response;

import java.util.UUID;
import java.util.*;
import java.util.stream.Collectors;

import static org.corfudb.runtime.protocol.proto.CorfuProtocol.TokenRequest.TokenRequestType;
import static org.corfudb.runtime.protocol.proto.CorfuProtocol.TokenRequest.TokenRequestType.*;
import static org.corfudb.runtime.protocol.proto.CorfuProtocol.TokenRequest.newBuilder;

/**
 * Created by Maithem on 7/1/20.
 */
public class API {

    public static final ProtocolVersion CURRENT_VERSION = ProtocolVersion.v0;
    public static final UUID DEFAULT_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

    // Temporary message header markers indicating message type.
    public static final byte LEGACY_CORFU_MSG_MARK = 0x1;
    public static final byte PROTO_CORFU_MSG_MARK = 0x2;

    public static CorfuProtocol.UUID getUUID(UUID uuid) {
        return CorfuProtocol.UUID.newBuilder()
                .setLsb(uuid.getLeastSignificantBits())
                .setMsb(uuid.getMostSignificantBits())
                .build();
    }

    public static UUID getJavaUUID(CorfuProtocol.UUID uuid) {
        return new UUID(uuid.getLsb(), uuid.getMsb());
    }

    public static Header getHeader(long requestId, Priority priority, MessageType type, long epoch,
                                   CorfuProtocol.UUID clusterId, CorfuProtocol.UUID clientId,
                                   boolean ignoreClusterId, boolean ignoreEpoch) {
        return Header.newBuilder()
                .setVersion(CURRENT_VERSION)
                .setRequestId(requestId)
                .setPriority(priority)
                .setType(type)
                .setEpoch(epoch)
                .setClusterId(clusterId)
                .setClientId(clientId)
                .setIgnoreClusterId(ignoreClusterId)
                .setIgnoreEpoch(ignoreEpoch)
                .build();
    }

    public static Header getHeader(long requestId, Priority priority, MessageType type,
                                   long epoch, UUID clusterId, UUID clientId,
                                   boolean ignoreClusterId, boolean ignoreEpoch) {
        return getHeader(requestId, priority, type, epoch,
                getUUID(clusterId), getUUID(clientId), ignoreClusterId, ignoreEpoch);
    }

    public static Header generateResponseHeader(Header requestHeader, boolean ignoreClusterId, boolean ignoreEpoch) {
        return getHeader(requestHeader.getRequestId(),
                requestHeader.getPriority(),
                requestHeader.getType(),
                requestHeader.getEpoch(),
                requestHeader.getClusterId(),
                requestHeader.getClientId(),
                ignoreClusterId,
                ignoreEpoch);
    }

    public static ServerError getNoServerError() {
        return ServerError.newBuilder()
                .setCode(ERROR.OK)
                .build();
    }

    public static ServerError getWrongEpochServerError(long serverEpoch) {
        return ServerError.newBuilder()
                .setCode(ERROR.WRONG_EPOCH)
                .setWrongEpochPayload(serverEpoch)
                .build();
    }

    public static ServerError getNotReadyServerError() {
        return ServerError.newBuilder()
                .setCode(ERROR.NOT_READY)
                .build();
    }

    public static ServerError getWrongClusterServerError(CorfuProtocol.UUID serverClusterId,
                                                         CorfuProtocol.UUID clientClusterId) {
        return ServerError.newBuilder()
                .setCode(ERROR.WRONG_CLUSTER)
                .setWrongClusterPayload(WrongClusterPayload.newBuilder()
                        .setServerClusterId(serverClusterId)
                        .setClientClusterId(clientClusterId)
                        .build())
                .build();
    }

    public static ServerError getBootstrappedServerError() {
        return ServerError.newBuilder()
                .setCode(ERROR.BOOTSTRAPPED)
                .build();
    }

    public static ServerError getNotBootstrappedServerError() {
        return ServerError.newBuilder()
                .setCode(ERROR.NOT_BOOTSTRAPPED)
                .build();
    }

    public static Request getPingRequest(Header header) {
        PingRequest pingRequest = PingRequest.getDefaultInstance();
        return Request.newBuilder()
                .setHeader(header)
                .setPingRequest(pingRequest)
                .build();
    }

    public static Response getPingResponse(Header header) {
        return getPingResponse(header, getNoServerError());
    }

    public static Response getPingResponse(Header header, ServerError error) {
        PingResponse pingResponse = PingResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .setPingResponse(pingResponse)
                .build();
    }

    public static Request getRestartRequest(Header header) {
        RestartRequest restartRequest = RestartRequest.getDefaultInstance();
        return Request.newBuilder()
                .setHeader(header)
                .setRestartRequest(restartRequest)
                .build();
    }

    public static Response getRestartResponse(Header header) {
        return getRestartResponse(header, getNoServerError());
    }

    public static Response getRestartResponse(Header header, ServerError error) {
        RestartResponse restartResponse = RestartResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .setRestartResponse(restartResponse)
                .build();
    }

    public static Request getResetRequest(Header header) {
        ResetRequest resetRequest = ResetRequest.getDefaultInstance();
        return Request.newBuilder()
                .setHeader(header)
                .setResetRequest(resetRequest)
                .build();
    }

    public static Response getResetResponse(Header header) {
        return getResetResponse(header, getNoServerError());
    }

    public static Response getResetResponse(Header header, ServerError error) {
        ResetResponse resetResponse = ResetResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .setResetResponse(resetResponse)
                .build();
    }

    public static Request getSealRequest(Header header, long newEpoch) {
        SealRequest sealRequest = SealRequest.newBuilder()
                .setEpoch(newEpoch)
                .build();
        return Request.newBuilder()
                .setHeader(header)
                .setSealRequest(sealRequest)
                .build();
    }

    public static Response getSealResponse(Header header) {
        return getSealResponse(header, getNoServerError());
    }

    public static Response getSealResponse(Header header, ServerError error) {
        SealResponse sealResponse = SealResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .setSealResponse(sealResponse)
                .build();
    }

    /**
     * Create a new TokenRequest with the parameters.
     *
     * @param numTokens number of tokens
     * @param streams streams containing the Java UUIDs
     * @param conflictInfo conflict TxResolutionInfo
     * @param tokenRequestType The tokenRequestType indicating the scenario of the token request
     * @return new TokenRequest proto object.
     */
    public static TokenRequest getTokenRequest(Long numTokens, List<UUID> streams,
                                               TxResolutionInfo conflictInfo,
                                               TokenRequestType tokenRequestType){
        // converting java UUID streams to Proto UUID streams
        List<CorfuProtocol.UUID> protoStreams = new ArrayList<>();
        if (streams!=null)
            streams.forEach((uuid -> protoStreams.add(getUUID(uuid))));

        return newBuilder()
                .setRequestType(tokenRequestType)
                .setNumTokens(numTokens)
                .addAllStreams(protoStreams)
                .setTxnResolution(conflictInfo)
                .build();
    }

    /**
     * Create a new TokenRequest with the parameters.
     *
     * @param numTokens number of tokens
     * @param streams streams containing the Java UUIDs
     * @param conflictInfo conflict TxResolutionInfo
     * @return new TokenRequest proto object.
     */
    public static TokenRequest getTokenRequest(Long numTokens, List<UUID> streams,
                                               TxResolutionInfo conflictInfo){
        return getTokenRequest(numTokens, streams, conflictInfo, TK_TX);
    }

    /**
     * Create a new TokenRequest with the parameters.
     *
     * @param numTokens number of tokens
     * @param streams streams containing the Java UUIDs
     * @return new TokenRequest proto object.
     */
    public static TokenRequest getTokenRequest(Long numTokens, List<UUID> streams){
        if (numTokens == 0) {
            return getTokenRequest(numTokens, streams, null, TK_QUERY);
        } else if (streams == null || streams.isEmpty()) {
            return getTokenRequest(numTokens, null, null, TK_RAW);
        } else {
            return getTokenRequest(numTokens, streams, null, TK_MULTI_STREAM);
        }
    }

    /**
     * Given two tokens return the comparator value
     * @param a first token
     * @param b second token
     * @return the value 0 if a == b;
     *          a value less than 0 if a < b;
     *          and a value greater than 0 if a > b
     */
    public static int compareToken(Token a, Token b) {
        int epochCmp = Long.compare(a.getEpoch(), b.getEpoch());
        if (epochCmp == 0) {
            return Long.compare(a.getSequence(), b.getSequence());
        }
        return epochCmp;
    }

    /**
     * Given two tokens return the min token
     * @param a first token
     * @param b second token
     * @return the reference to the min token
     */
    public static Token getMinToken(Token a, Token b) {
        if (compareToken(a,b) <= 0) {
            return a;
        } else {
            return b;
        }
    }

    public static Token getToken(long epoch, long sequence){
        return Token.newBuilder()
                .setEpoch(epoch)
                .setSequence(sequence)
                .build();
    }

    public static Token getUninitializedToken(){
        return getToken(Address.NON_ADDRESS, Address.NON_ADDRESS);
    }

    /**
     * Create a new TxResolutionInfo with the parameters.
     *
     * @param txId transaction identifier
     * @param snapshotTimestamp transaction snapshot timestamp
     * @param conflictMap map of conflict parameters, arranged by stream IDs
     * @param writeConflictParamsSet map of write conflict parameters, arranged by stream IDs
     *
     * @return new TxResolutionInfo proto object.
     */
    public static TxResolutionInfo getTxResolutionInfo(UUID txId, Token snapshotTimestamp,
            Map<UUID, Set<byte[]>> conflictMap, Map<UUID, Set<byte[]>> writeConflictParamsSet) {

        // converting conflictMap of java Map<UUID, Set<byte[]>>
        // to a Map of Proto UUIDToListOfBytesMap with entries as UUIDToListOfBytesPair
        UUIDToListOfBytesMap.Builder protoConflictMapBuilder = UUIDToListOfBytesMap.newBuilder();

        conflictMap.forEach((uuid, bytes) -> {
            // Create a List of ByteStrings for each UUID
            // ByteString = Protobuf's Immutable sequence of bytes (similar to byte[])
            List<ByteString> byteStringList = new ArrayList<>();

            // Parse the Set of array of bytes(byte[]) for each UUID and
            // create a ByteString for each entry(byte[]) and add to byteStringList
            bytes.forEach(bytes1 -> byteStringList.add(ByteString.copyFrom(bytes1)));

            // Add the newly created entry of UUIDToListOfBytesPair to the UUIDToListOfBytesMap builder
            protoConflictMapBuilder.addEntries(
                    UUIDToListOfBytesPair.newBuilder()
                            .setKey(getUUID(uuid))
                            .addAllValue(byteStringList)
                            .build()
            );
        });
        protoConflictMapBuilder.build();


        // converting writeConflictParamsSet of java Map<UUID, Set<byte[]>>
        // to a Map of Proto UUIDToListOfBytesMap with entries as UUIDToListOfBytesPair
        UUIDToListOfBytesMap.Builder protoWriteConflictParamsBuilder = UUIDToListOfBytesMap.newBuilder();
        writeConflictParamsSet.forEach((uuid, bytes) -> {
            // Create a List of ByteStrings for each UUID
            // ByteString = Protobuf's Immutable sequence of bytes (similar to byte[])
            List<ByteString> byteStringList = new ArrayList<>();

            // Parse the Set of array of bytes(byte[]) for each UUID and
            // create a ByteString for each entry(byte[]) and add to byteStringList
            bytes.forEach(bytes1 -> byteStringList.add(ByteString.copyFrom(bytes1)));

            // Add the newly created entry of UUIDToListOfBytesPair to the UUIDToListOfBytesMap builder
            protoWriteConflictParamsBuilder.addEntries(
                                    UUIDToListOfBytesPair.newBuilder()
                                            .setKey(getUUID(uuid))
                                            .addAllValue(byteStringList)
                                            .build()
            );
        });
        protoWriteConflictParamsBuilder.build();

        return TxResolutionInfo.newBuilder()
                .setTXid(getUUID(txId))
                .setSnapshotTimestamp(snapshotTimestamp)
                .setConflictSet(protoConflictMapBuilder)
                .setWriteConflictParamsSet(protoWriteConflictParamsBuilder)
                .build();
    }

    /**
     * Create a new TxResolutionInfo with the parameters.
     *
     * @param txId transaction identifier
     * @param snapshotTimestamp transaction snapshot timestamp
     */
    public static TxResolutionInfo getTxResolutionInfo(UUID txId, Token snapshotTimestamp) {
        return getTxResolutionInfo(txId, snapshotTimestamp, Collections.emptyMap(),
                Collections.emptyMap());
    }

    public static final byte[] TOKEN_RESPONSE_NO_CONFLICT_KEY = new byte[]{ 0 };
    public static final UUID TOKEN_RESPONSE_NO_CONFLICT_STREAM = new UUID(0, 0);

    public static UUIDToLongMap getProtoUUIDToLongMap(Map<UUID, Long> javaUuidLongMap){
        UUIDToLongMap.Builder uuidToLongMapBuilder = UUIDToLongMap.newBuilder();
        // For every entry in uuidLongMap,
        // create and add a new UUIDToLongPair to the UUIDToLongMap builder
        javaUuidLongMap.forEach((uuid, aLong) -> uuidToLongMapBuilder.addEntries(
                UUIDToLongPair.newBuilder().setKey(getUUID(uuid)).setValue(aLong).build()
        ));
        return uuidToLongMapBuilder.build();
    }

    /**
     * Create a new TokenResponse with the parameters.
     *
     * @param token token value
     * @param backPointerMap  map of backPointers for all requested streams
     */
    public static CorfuProtocol.TokenResponse getTokenResponse(CorfuProtocol.Token token,
                                                               UUIDToLongMap backPointerMap){
        return getTokenResponse(
                TokenType.TX_NORMAL,
                ByteString.copyFrom(TOKEN_RESPONSE_NO_CONFLICT_KEY),
                getUUID(TOKEN_RESPONSE_NO_CONFLICT_STREAM),
                token,
                backPointerMap,
                UUIDToLongMap.getDefaultInstance()
        );
    }

    /**
     * Create a new TokenResponse with the parameters.
     *
     * @param tokenType token type
     * @param conflictingKey the key responsible for the conflict
     * @param conflictingStream the stream responsible for the conflict
     * @param token token value
     * @param backPointerMap map of backPointers for all requested streams
     * @param streamTails map of streamTails
     * @return new TokenResponse proto object
     */
    public static CorfuProtocol.TokenResponse getTokenResponse(TokenType tokenType,
               ByteString conflictingKey, CorfuProtocol.UUID conflictingStream, CorfuProtocol.Token token,
               UUIDToLongMap backPointerMap, UUIDToLongMap streamTails){

        return TokenResponse.newBuilder()
                .setRespType(tokenType)
                .setConflictKey(conflictingKey)
                .setConflictStream(conflictingStream)
                .setToken(token)
                .setBackPointerMap(backPointerMap)
                .setStreamTails(streamTails)
                .build();
    }

    /**
     * Create a new {@link CorfuProtocol.Response} proto object with the parameters.
     *
     * @param tokenResponse token response proto object
     * @return new {@link CorfuProtocol.Response} proto object
     */
    public static CorfuProtocol.Response getTokenResponse(Header header, TokenResponse tokenResponse){
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setTokenResponse(tokenResponse)
                .build();
    }

    /**
     * Create a new TxResolutionResponse with the parameters.
     *
     * @param tokenType response type from the sequencer
     */
    public static TxResolutionResponse getTxResolutionResponse(CorfuProtocol.TokenType tokenType,
           long keyAddress, ByteString conflictParam, CorfuProtocol.UUID conflictStream) {
        return TxResolutionResponse.newBuilder()
                .setTokenType(tokenType)
                .setAddress(Address.NON_ADDRESS)
                .setConflictingKey(conflictParam)
                .setConflictingStream(conflictStream)
                .build();
    }

    /**
     * Create a new TxResolutionResponse with the parameters.
     *
     * @param tokenType response type from the sequencer
     */
    public static TxResolutionResponse getTxResolutionResponse(CorfuProtocol.TokenType tokenType) {
        return getTxResolutionResponse(
                tokenType,
                Address.NON_ADDRESS,
                ByteString.copyFrom(TOKEN_RESPONSE_NO_CONFLICT_KEY),
                getUUID(TOKEN_RESPONSE_NO_CONFLICT_STREAM));
    }

    public static StreamAddressRange getSteamAddressRange(CorfuProtocol.UUID streamID, long start,
                                                         long end){
        return StreamAddressRange.newBuilder()
                .setStreamID(streamID)
                .setStart(start)
                .setEnd(end)
                .build();
    }

    public static StreamsAddressRequest getStreamsAddressRequest(@NonNull List<StreamAddressRange> streamsRanges){
        return StreamsAddressRequest.newBuilder()
                .setReqType(StreamsAddressRequest.Type.STREAMS)
                .addAllStreamsRanges(streamsRanges)
                .build();
    }

    public static Request getSequencerTrimRequest(Header header, long trimMark) {
        SequencerTrimRequest sequencerTrimRequest = SequencerTrimRequest.newBuilder()
                .setTrimMark(trimMark)
                .build();
        return Request.newBuilder()
                .setHeader(header)
                .setSequencerTrimRequest(sequencerTrimRequest)
                .build();
    }

    public static SequencerMetrics getSequencerMetrics(SequencerMetrics.SequencerStatus sequencerStatus){
        return SequencerMetrics.newBuilder()
                .setSequencerStatus(sequencerStatus)
                .build();
    }

    public static Response getSequencerTrimResponse(Header header) {
        SequencerTrimResponse sequencerTrimResponse = SequencerTrimResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setSequencerTrimResponse(sequencerTrimResponse)
                .build();
    }

    public static Request getSequencerMetricsRequest(Header header) {
        SequencerMetricsRequest sequencerMetricsRequest = SequencerMetricsRequest.getDefaultInstance();
        return Request.newBuilder()
                .setHeader(header)
                .setSequencerMetricsRequest(sequencerMetricsRequest)
                .build();
    }

    public static Response getSequencerMetricsResponse(Header header,
                                                       SequencerMetrics sequencerMetrics) {
        SequencerMetricsResponse sequencerMetricsResponse = SequencerMetricsResponse
                .newBuilder()
                .setSequencerMetrics(sequencerMetrics)
                .build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setSequencerMetricsResponse(sequencerMetricsResponse)
                .build();
    }

    public static Request getBootStrapSequencerRequest(Header header, long globalTail,
            UUIDToStreamAddressMap streamAddressMap, long sequencerEpoch,
                                                       boolean bootstrapWithoutTailsUpdate) {
        BootStrapSequencerRequest bootStrapSequencerRequest = BootStrapSequencerRequest.newBuilder()
                .setGlobalTail(globalTail)
                .setStreamsAddressMap(streamAddressMap)
                .setSequencerEpoch(globalTail)
                .setBootstrapWithoutTailsUpdate(bootstrapWithoutTailsUpdate)
                .build();
        return Request.newBuilder()
                .setHeader(header)
                .setBootStrapSequencerRequest(bootStrapSequencerRequest)
                .build();
    }

    public static Response getBootStrapSequencerResponse(Header header) {
        BootStrapSequencerResponse bootStrapSequencerResponse = BootStrapSequencerResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setBootStrapSequencerResponse(bootStrapSequencerResponse)
                .build();
    }

    public static StreamsAddressResponse getStreamsAddressResponse(long logTail, UUIDToStreamAddressMap streamsAddressesMap) {
        return StreamsAddressResponse.newBuilder()
                .setLogTail(logTail)
                .setAddressMap(streamsAddressesMap)
                .build();
    }

    public static Response getStreamsAddressResponse(Header header, long logTail, UUIDToStreamAddressMap streamsAddressesMap) {
        StreamsAddressResponse streamsAddressResponse =
                getStreamsAddressResponse(logTail, streamsAddressesMap);
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setStreamsAddressResponse(streamsAddressResponse)
                .build();
    }

    // Layout Messages API

    public static Layout toProtobufLayout(org.corfudb.runtime.view.Layout layout) {
        return Layout.newBuilder()
                .setLayoutJSON(layout.asJSONString())
                .build();

    }

    public static org.corfudb.runtime.view.Layout fromProtobufLayout(Layout protobufLayout) {
        return org.corfudb.runtime.view.Layout.fromJSONString(protobufLayout.getLayoutJSON());
    }

    public static Request getGetLayoutRequest(Header header, long epoch) {
        GetLayoutRequest getLayoutRequest = GetLayoutRequest.newBuilder().setEpoch(epoch).build();
        return Request.newBuilder()
                .setHeader(header)
                .setGetLayoutRequest(getLayoutRequest)
                .build();
    }

    public static Response getGetLayoutResponse(Header header, org.corfudb.runtime.view.Layout layout) {
        GetLayoutResponse getLayoutResponse = GetLayoutResponse.newBuilder()
                .setLayout(toProtobufLayout(layout))
                .build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setGetLayoutResponse(getLayoutResponse)
                .build();
    }

    public static Request getPrepareLayoutRequest(Header header, long epoch, long rank) {
        PrepareLayoutRequest prepareLayoutRequest = PrepareLayoutRequest.newBuilder()
                .setEpoch(epoch)
                .setRank(rank)
                .build();
        return Request.newBuilder()
                .setHeader(header)
                .setPrepareLayoutRequest(prepareLayoutRequest)
                .build();
    }

    public static Response getPrepareLayoutResponse(Header header, PrepareLayoutResponse.Type type,
                                                    long rank, org.corfudb.runtime.view.Layout layout) {
        PrepareLayoutResponse prepareLayoutResponse = PrepareLayoutResponse.newBuilder()
                .setRespType(type)
                .setRank(rank)
                .setLayout(toProtobufLayout(layout))
                .build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setPrepareLayoutResponse(prepareLayoutResponse)
                .build();
    }

    public static Request getProposeLayoutRequest(Header header, long epoch, long rank,
                                                  org.corfudb.runtime.view.Layout layout) {
        ProposeLayoutRequest proposeLayoutRequest = ProposeLayoutRequest.newBuilder()
                .setEpoch(epoch)
                .setRank(rank)
                .setLayout(toProtobufLayout(layout))
                .build();
        return Request.newBuilder()
                .setHeader(header)
                .setProposeLayoutRequest(proposeLayoutRequest)
                .build();
    }

    public static Response getProposeLayoutResponse(Header header, ProposeLayoutResponse.Type type, long rank) {
        ProposeLayoutResponse proposeLayoutResponse =
                ProposeLayoutResponse.newBuilder().setRespType(type).setRank(rank).build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setProposeLayoutResponse(proposeLayoutResponse)
                .build();
    }

    public static Request getCommitLayoutRequest(Header header, boolean forced, long epoch,
                                                 org.corfudb.runtime.view.Layout layout) {
        CommitLayoutRequest commitLayoutRequest = CommitLayoutRequest.newBuilder()
                .setForced(forced)
                .setEpoch(epoch)
                .setLayout(toProtobufLayout(layout))
                .build();
        return Request.newBuilder()
                .setHeader(header)
                .setCommitLayoutRequest(commitLayoutRequest)
                .build();
    }

    public static Response getCommitLayoutResponse(Header header, CommitLayoutResponse.Type type) {
        CommitLayoutResponse commitLayoutResponse =
                CommitLayoutResponse.newBuilder().setRespType(type).build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setCommitLayoutResponse(commitLayoutResponse)
                .build();
    }

    public static Request getBootstrapLayoutRequest(Header header, org.corfudb.runtime.view.Layout layout) {
        BootstrapLayoutRequest bootstrapLayoutRequest =
                BootstrapLayoutRequest.newBuilder().setLayout(toProtobufLayout(layout)).build();
        return Request.newBuilder()
                .setHeader(header)
                .setBootstrapLayoutRequest(bootstrapLayoutRequest)
                .build();
    }

    public static Response getBootstrapLayoutResponse(Header header, BootstrapLayoutResponse.Type type) {
        BootstrapLayoutResponse bootstrapLayoutResponse =
                BootstrapLayoutResponse.newBuilder().setRespType(type).build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setBootstrapLayoutResponse(bootstrapLayoutResponse)
                .build();
    }

    // Management Messages API

    public static Response getBootstrapManagementResponse(Header header, BootstrapManagementResponse.Type type) {
        BootstrapManagementResponse bootstrapManagementResponse =
                BootstrapManagementResponse.newBuilder().setRespType(type).build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setBootstrapManagementResponse(bootstrapManagementResponse)
                .build();
    }

    public static Response getReportFailureResponse(Header header, ReportFailureResponse.Type type) {
        ReportFailureResponse reportFailureResponse =
                ReportFailureResponse.newBuilder().setRespType(type).build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setReportFailureResponse(reportFailureResponse)
                .build();
    }

    public static Response getHealFailureResponse(Header header, HealFailureResponse.Type type) {
        HealFailureResponse healFailureResponse =
                HealFailureResponse.newBuilder().setRespType(type).build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setHealFailureResponse(healFailureResponse)
                .build();
    }

    public static Response getGetManagementLayoutResponse(Header header, org.corfudb.runtime.view.Layout layout) {
        GetManagementLayoutResponse getManagementLayoutResponse =
                GetManagementLayoutResponse.newBuilder().setLayout(toProtobufLayout(layout)).build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setGetManagementLayoutResponse(getManagementLayoutResponse)
                .build();
    }

    public static NodeConnectivity toProtobufNodeConnectivity(
            org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity nc) {
        return NodeConnectivity.newBuilder()
                .setEndpoint(nc.getEndpoint())
                .setConnectivityType(nc.getType().name())
                .setEpoch(nc.getEpoch())
                .setConnectivityMap(Entries.newBuilder()
                        .addAllItems(nc.getConnectivity()
                                .entrySet()
                                .stream()
                                .map(e -> Any.pack(NodeConnectivity.ConnectivityMapEntry
                                        .newBuilder()
                                        .setKey(e.getKey())
                                        .setValue(e.getValue().name())
                                        .build()))
                                .collect(Collectors.toList()))
                        .build())
                .build();
    }

    public static Response getQueryNodeResponse(Header header, NodeState state) {
        //TODO(Zach): setSequencerMetrics
        QueryNodeResponse queryNodeResponse = QueryNodeResponse.newBuilder()
                .setNodeConnectivity(toProtobufNodeConnectivity(state.getConnectivity()))
                .build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setQueryNodeResponse(queryNodeResponse)
                .build();
    }

    // Orchestrator Messages API

    public static Response getQueryWorkflowResponse(Header header, boolean active) {
        OrchestratorResponse orchestratorResponse = OrchestratorResponse.newBuilder()
                .setQueryWorkflow(OrchestratorResponse.QueryWorkflow.newBuilder().setIsActive(active).build())
                .build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setOrchestratorResponse(orchestratorResponse)
                .build();
    }

    public static Response getCreateWorkflowResponse(Header header, UUID id) {
        OrchestratorResponse orchestratorResponse = OrchestratorResponse.newBuilder()
                .setCreateWorkflow(OrchestratorResponse.CreateWorkflow.newBuilder().setWorkflowId(getUUID(id)).build())
                .build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setOrchestratorResponse(orchestratorResponse)
                .build();
    }

    public static AddNodeRequest getJavaAddNodeRequest(OrchestratorRequest.ExecuteWorkflow workflowReq) {
        return new AddNodeRequest(workflowReq.getEndpoint());
    }

    public static RemoveNodeRequest getJavaRemoveNodeRequest(OrchestratorRequest.ExecuteWorkflow workflowReq) {
        return new RemoveNodeRequest(workflowReq.getEndpoint());
    }

    public static ForceRemoveNodeRequest getJavaForceRemoveNodeRequest(OrchestratorRequest.ExecuteWorkflow workflowReq) {
        return new ForceRemoveNodeRequest(workflowReq.getEndpoint());
    }

    public static HealNodeRequest getJavaHealNodeRequest(OrchestratorRequest.ExecuteWorkflow workflowReq) {
        return new HealNodeRequest(workflowReq.getEndpoint(),
                workflowReq.getLayoutServer(),
                workflowReq.getSequencerServer(),
                workflowReq.getLogUnitServer(),
                workflowReq.getStripeIndex());
    }

    public static RestoreRedundancyMergeSegmentsRequest getJavaRestoreRedundancyMergeSegmentsRequest(
            OrchestratorRequest.ExecuteWorkflow workflowReq) {
        return new RestoreRedundancyMergeSegmentsRequest(workflowReq.getEndpoint());
    }

    // LogUnit Messages RPCs

    public static Response getTrimMarkResponse(Header header, long trimMark) {
        TrimMarkResponse trimMarkResponse = TrimMarkResponse.newBuilder().setTrimMark(trimMark).build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setTrimMarkResponse(trimMarkResponse)
                .build();
    }

    public static Response getCommittedTailResponse(Header header, long committedTail) {
        CommittedTailResponse committedTailResponse =
                CommittedTailResponse.newBuilder().setCommittedTail(committedTail).build();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setCommittedTailResponse(committedTailResponse)
                .build();
    }

    public static Response getUpdateCommittedTailResponse(Header header) {
        UpdateCommittedTailResponse updateCommittedTailResponse = UpdateCommittedTailResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(getNoServerError())
                .setUpdateCommittedTailResponse(updateCommittedTailResponse)
                .build();
    }

    // Misc. API

    public static Response getErrorResponseNoPayload(Header header, ServerError error) {
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .build();
    }

    //TODO(Zach): Improve this method
    public static boolean validateRequestPayloadType(Request request) {
        switch(request.getHeader().getType()) {
            case PING:
                return request.hasPingRequest();
            case AUTHENTICATE:
                return request.hasAuthenticateRequest();
            case RESTART:
                return request.hasRestartRequest();
            case RESET:
                return request.hasResetRequest();
            case SEAL:
                return request.hasSealRequest();
            case GET_LAYOUT:
                return request.hasGetLayoutRequest();
            case PREPARE_LAYOUT:
                return request.hasPrepareLayoutRequest();
            case PROPOSE_LAYOUT:
                return request.hasProposeLayoutRequest();
            case COMMIT_LAYOUT:
                return request.hasCommitLayoutRequest();
            case BOOTSTRAP_LAYOUT:
                return request.hasBootstrapLayoutRequest();
            case TOKEN:
                return request.hasTokenRequest();
            case BOOTSTRAP_SEQUENCER:
                return request.hasBootStrapSequencerRequest();
            case SEQUENCER_TRIM:
                return request.hasSequencerTrimRequest();
            case SEQUENCER_METRICS:
                return request.hasSequencerMetricsRequest();
            case STREAMS_ADDRESS:
                return request.hasStreamsAddressRequest();
            case WRITE_LOG:
                return request.hasWriteLogRequest();
            case READ_LOG:
                return request.hasReadLogRequest();
            case INSPECT_ADDRESSES:
                return request.hasInspectAddressesRequest();
            case TRIM_LOG:
                return request.hasTrimLogRequest();
            case TRIM_MARK:
                return request.hasTrimMarkRequest();
            case TAIL:
                return request.hasTailRequest();
            case COMPACT_LOG:
                return request.hasCompactRequest();
            case FLUSH_CACHE:
                return request.hasFlushCacheRequest();
            case LOG_ADDRESS_SPACE:
                return request.hasLogAddressSpaceRequest();
            case KNOWN_ADDRESS:
                return request.hasKnownAddressRequest();
            case COMMITTED_TAIL:
                return request.hasCommittedTailRequest();
            case UPDATE_COMMITTED_TAIL:
                return request.hasUpdateCommittedTailRequest();
            case RESET_LOG_UNIT:
                return request.hasResetLogUnitRequest();
            case QUERY_NODE:
                return request.hasQueryNodeRequest();
            case REPORT_FAILURE:
                return request.hasReportFailureRequest();
            case HEAL_FAILURE:
                return request.hasHealFailureRequest();
            case ORCHESTRATOR:
                return request.hasOrchestratorRequest();
            case BOOTSTRAP_MANAGEMENT:
                return request.hasBootstrapManagementRequest();
            case GET_MANAGEMENT_LAYOUT:
                return request.hasGetManagementLayoutRequest();
            default:
                break;
        }

        return false;
    }
}
