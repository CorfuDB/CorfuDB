package org.corfudb.common.protocol;

import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.MessageType;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;
import org.corfudb.common.protocol.proto.CorfuProtocol.PingRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.PingResponse;
import org.corfudb.common.protocol.proto.CorfuProtocol.Priority;
import org.corfudb.common.protocol.proto.CorfuProtocol.ProtocolVersion;
import org.corfudb.common.protocol.proto.CorfuProtocol.ERROR;
import org.corfudb.common.protocol.proto.CorfuProtocol.ServerError;
import org.corfudb.common.protocol.proto.CorfuProtocol.ErrorNoPayload;
import org.corfudb.common.protocol.proto.CorfuProtocol.WrongClusterPayload;

import java.util.UUID;

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

    public static Header newHeader(long requestId, Priority priority, MessageType type,
                                   long epoch, UUID clusterId, UUID clientId,
                                   boolean ignoreClusterId, boolean ignoreEpoch) {
        return Header.newBuilder()
                .setVersion(CURRENT_VERSION)
                .setRequestId(requestId)
                .setPriority(priority)
                .setType(type)
                .setEpoch(epoch)
                .setClusterId(getUUID(clusterId))
                .setClientId(getUUID(clientId))
                .setIgnoreClusterId(ignoreClusterId)
                .setIgnoreEpoch(ignoreEpoch)
                .build();
    }

    public static ServerError newNoServerError() {
        return ServerError.newBuilder()
                .setCode(ERROR.OK)
                .setMessage("")
                .setNone(ErrorNoPayload.getDefaultInstance())
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
                .setNone(ErrorNoPayload.getDefaultInstance())
                .build();
    }

    public static ServerError newWrongClusterServerError(String errorMsg, UUID serverClusterId, UUID clientClusterId) {
        return ServerError.newBuilder()
                .setCode(ERROR.WRONG_CLUSTER)
                .setMessage(errorMsg)
                .setWrongClusterPayload(WrongClusterPayload.newBuilder()
                                        .setServerClusterId(getUUID(serverClusterId))
                                        .setClientClusterId(getUUID(clientClusterId))
                                        .build())
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
}
