package org.corfudb.common.protocol;

import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.MessageType;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;
import org.corfudb.common.protocol.proto.CorfuProtocol.PingRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.PingResponse;
import org.corfudb.common.protocol.proto.CorfuProtocol.RestartRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.RestartResponse;
import org.corfudb.common.protocol.proto.CorfuProtocol.ResetRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.ResetResponse;
import org.corfudb.common.protocol.proto.CorfuProtocol.SealRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.SealResponse;
import org.corfudb.common.protocol.proto.CorfuProtocol.PingResponse;
import org.corfudb.common.protocol.proto.CorfuProtocol.AuthenticateRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.AuthenticateResponse;
import org.corfudb.common.protocol.proto.CorfuProtocol.Priority;
import org.corfudb.common.protocol.proto.CorfuProtocol.ProtocolVersion;
import org.corfudb.common.protocol.proto.CorfuProtocol.StreamAddressRange;
import org.corfudb.common.protocol.proto.CorfuProtocol.QueryStreamRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.GetLayoutRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.PrepareLayoutRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.ERROR;
import org.corfudb.common.protocol.proto.CorfuProtocol.ServerError;
import org.corfudb.common.protocol.proto.CorfuProtocol.WrongClusterPayload;

import java.util.List;
import java.util.UUID;

import static org.corfudb.common.protocol.proto.CorfuProtocol.*;

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

    public static Header newHeader(long requestId, Priority priority, MessageType type, long epoch,
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

    public static Header newHeader(long requestId, Priority priority, MessageType type,
                                   long epoch, UUID clusterId, UUID clientId,
                                   boolean ignoreClusterId, boolean ignoreEpoch) {
        return newHeader(requestId, priority, type, epoch,
                getUUID(clusterId), getUUID(clientId), ignoreClusterId, ignoreEpoch);
    }

    public static Header generateResponseHeader(Header requestHeader, boolean ignoreClusterId, boolean ignoreEpoch) {
        return newHeader(requestHeader.getRequestId(),
                requestHeader.getPriority(),
                requestHeader.getType(),
                requestHeader.getEpoch(),
                requestHeader.getClusterId(),
                requestHeader.getClientId(),
                ignoreClusterId,
                ignoreEpoch);
    }

    public static ServerError newNoServerError() {
        return ServerError.newBuilder()
                .setCode(ERROR.OK)
                .setMessage("")
                .build();
    }

    public static ServerError newWrongEpochServerError(String errorMsg, long serverEpoch) {
        return ServerError.newBuilder()
                .setCode(ERROR.WRONG_EPOCH)
                .setMessage(errorMsg)
                .setWrongEpochPayload(serverEpoch)
                .build();
    }

    public static ServerError newNotReadyServerError(String errorMsg) {
        return ServerError.newBuilder()
                .setCode(ERROR.NOT_READY)
                .setMessage(errorMsg)
                .build();
    }

    public static ServerError newWrongClusterServerError(String errorMsg, CorfuProtocol.UUID serverClusterId,
                                                         CorfuProtocol.UUID clientClusterId) {
        return ServerError.newBuilder()
                .setCode(ERROR.WRONG_CLUSTER)
                .setMessage(errorMsg)
                .setWrongClusterPayload(WrongClusterPayload.newBuilder()
                        .setServerClusterId(serverClusterId)
                        .setClientClusterId(clientClusterId)
                        .build())
                .build();
    }

    public static ServerError newNotBootstrappedServerError(String errorMsg) {
        return ServerError.newBuilder()
                .setCode(ERROR.NOT_BOOTSTRAPPED)
                .setMessage(errorMsg)
                .build();
    }

    public static Request newPingRequest(Header header) {
        PingRequest pingRequest = PingRequest.getDefaultInstance();
        return Request.newBuilder()
                .setHeader(header)
                .setPingRequest(pingRequest)
                .build();
    }

    public static Response newPingResponse(Header header) {
        return newPingResponse(header, newNoServerError());
    }

    public static Response newPingResponse(Header header, ServerError error) {
        PingResponse pingResponse = PingResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .setPingResponse(pingResponse)
                .build();
    }

    public static Request newRestartRequest(Header header) {
        RestartRequest restartRequest = RestartRequest.getDefaultInstance();
        return Request.newBuilder()
                .setHeader(header)
                .setRestartRequest(restartRequest)
                .build();
    }

    public static Response newRestartResponse(Header header) {
        return newRestartResponse(header, newNoServerError());
    }

    public static Response newRestartResponse(Header header, ServerError error) {
        RestartResponse restartResponse = RestartResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .setRestartResponse(restartResponse)
                .build();
    }

    public static Request newResetRequest(Header header) {
        ResetRequest resetRequest = ResetRequest.getDefaultInstance();
        return Request.newBuilder()
                .setHeader(header)
                .setResetRequest(resetRequest)
                .build();
    }

    public static Response newResetResponse(Header header) {
        return newRestartResponse(header, newNoServerError());
    }

    public static Response newResetResponse(Header header, ServerError error) {
        ResetResponse resetResponse = ResetResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .setResetResponse(resetResponse)
                .build();
    }

    public static Request newSealRequest(Header header, long newEpoch) {
        SealRequest sealRequest = SealRequest.newBuilder()
                                        .setEpoch(newEpoch)
                                        .build();
        return Request.newBuilder()
                .setHeader(header)
                .setSealRequest(sealRequest)
                .build();
    }

    public static Response newSealResponse(Header header) {
        return newSealResponse(header, newNoServerError());
    }

    public static Response newSealResponse(Header header, ServerError error) {
        SealResponse sealResponse = SealResponse.getDefaultInstance();
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .setSealResponse(sealResponse)
                .build();
    }

    public static Response newErrorResponseNoPayload(Header header, ServerError error) {
        return Response.newBuilder()
                .setHeader(header)
                .setError(error)
                .build();
    }
    public static Request newAuthenticateRequest(Header header, UUID clientId, UUID serverId) {
        AuthenticateRequest authRequest = AuthenticateRequest.newBuilder()
                                            .setClientId(getUUID(clientId))
                                            .setServerId(getUUID(serverId))
                                            .build();
        return Request.newBuilder()
                .setHeader(header)
                .setAuthenticateRequest(authRequest)
                .build();
    }

    public static Response newAuthenticateResponse(Header header, UUID serverId, String version) {
        AuthenticateResponse authResponse = AuthenticateResponse.newBuilder()
                                                .setServerId(getUUID(serverId))
                                                .setCorfuVersion(version)
                                                .build();
        return Response.newBuilder()
                .setHeader(header)
                .setAuthenticateResponse(authResponse)
                .build();
    }

    public static Request newGetLayoutRequest(Header header, long epoch) {
        GetLayoutRequest layoutRequest = GetLayoutRequest.newBuilder().setEpoch(epoch).build();
        return Request.newBuilder()
                .setHeader(header)
                .setGetLayoutRequest(layoutRequest)
                .build();
    }

    public static Request newPrepareLayoutRequest(Header header,long epoch, long rank) {
        PrepareLayoutRequest prepLayoutRequest = PrepareLayoutRequest.newBuilder()
                                                            .setEpoch(epoch)
                                                            .setRank(rank)
                                                            .build();
        return Request.newBuilder()
                .setHeader(header)
                .setPrepareLayoutRequest(prepLayoutRequest)
                .build();
    }

    public static Request newQueryStreamRequest(Header header, QueryStreamRequest.ReqType type,
                                                List<StreamAddressRange> ranges) {
        QueryStreamRequest qsRequest = QueryStreamRequest.newBuilder()
                                                .setType(type)
                                                .addAllStreamRanges(ranges)
                                                .build();
        return Request.newBuilder()
                .setHeader(header)
                .setQueryStreamRequest(qsRequest)
                .build();
    }

    public static Request newQueryStreamRequest(Header header, List<StreamAddressRange> ranges) {
        return newQueryStreamRequest(header, QueryStreamRequest.ReqType.STREAMS, ranges);
    }

    public static Request newRestartRequest(Header header) {
        RestartRequest restartRequest = RestartRequest.getDefaultInstance();
        return Request.newBuilder()
                .setHeader(header)
                .setRestartRequest(restartRequest)
                .build();
    }
}
