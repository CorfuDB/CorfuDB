package org.corfudb.common.protocol;


import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.MessageType;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;
import org.corfudb.common.protocol.proto.CorfuProtocol.PingRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.AuthenticateRequest;
import org.corfudb.common.protocol.proto.CorfuProtocol.AuthenticateResponse;
import org.corfudb.common.protocol.proto.CorfuProtocol.Priority;
import org.corfudb.common.protocol.proto.CorfuProtocol.ProtocolVersion;

import java.util.UUID;

/**
 * Created by Maithem on 7/1/20.
 */

public class API {

    public static final ProtocolVersion CURRENT_VERSION = ProtocolVersion.v0;

    private static CorfuProtocol.UUID getUUID(UUID uuid) {
        return CorfuProtocol.UUID.newBuilder()
                .setLsb(uuid.getLeastSignificantBits())
                .setMsb(uuid.getMostSignificantBits())
                .build();
    }

    public static Header newHeader(long requestId,
                                   Priority priority, MessageType type,
                                   long epoch, UUID clusterId) {
        return Header.newBuilder()
                .setVersion(CURRENT_VERSION)
                .setRequestId(requestId)
                .setPriority(priority)
                .setType(type)
                .setEpoch(epoch)
                .setClusterId(getUUID(clusterId))
                .build();
    }

    public static Request newPingRequest(Header header) {
        PingRequest pingRequest = PingRequest.getDefaultInstance();
        return Request.newBuilder()
                .setHeader(header)
                .setPingRequest(pingRequest)
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
}
