package org.corfudb.protocols.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.VersionInfo;
import org.corfudb.runtime.proto.service.Base.HandshakeRequestMsg;
import org.corfudb.runtime.proto.service.Base.HandshakeResponseMsg;
import org.corfudb.runtime.proto.service.Base.PingRequestMsg;
import org.corfudb.runtime.proto.service.Base.PingResponseMsg;
import org.corfudb.runtime.proto.service.Base.ResetRequestMsg;
import org.corfudb.runtime.proto.service.Base.ResetResponseMsg;
import org.corfudb.runtime.proto.service.Base.RestartRequestMsg;
import org.corfudb.runtime.proto.service.Base.RestartResponseMsg;
import org.corfudb.runtime.proto.service.Base.SealRequestMsg;
import org.corfudb.runtime.proto.service.Base.SealResponseMsg;
import org.corfudb.runtime.proto.service.Base.VersionRequestMsg;
import org.corfudb.runtime.proto.service.Base.VersionResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;

import java.util.UUID;

/**
 * This class provides methods for creating the Protobuf objects defined
 * in base.proto. These provide the interface for performing the RPCs
 * handled by the Base server, as well as the handshake.
 */
@Slf4j
public class CorfuProtocolBase {
    // Prevent class from being instantiated
    private CorfuProtocolBase() {}

    /**
     * Returns a PING request message that can be sent by the client.
     */
    public static RequestPayloadMsg getPingRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setPingRequest(PingRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a PING response message that can be sent by the server.
     */
    public static ResponsePayloadMsg getPingResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setPingResponse(PingResponseMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a RESTART request message that can be sent by the client.
     */
    public static RequestPayloadMsg getRestartRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setRestartRequest(RestartRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a RESTART response message that can be sent by the server.
     */
    public static ResponsePayloadMsg getRestartResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setRestartResponse(RestartResponseMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a RESET request message that can be sent by the client.
     */
    public static RequestPayloadMsg getResetRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setResetRequest(ResetRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a RESET response message that can be sent by the server.
     */
    public static ResponsePayloadMsg getResetResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setResetResponse(ResetResponseMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a SEAL request message that can be sent by the client.
     *
     * @param epoch   the SEAL epoch
     */
    public static RequestPayloadMsg getSealRequestMsg(long epoch) {
        return RequestPayloadMsg.newBuilder()
                .setSealRequest(SealRequestMsg.newBuilder()
                        .setEpoch(epoch)
                        .build())
                .build();
    }

    /**
     * Returns a SEAL response message that can be sent by the server.
     */
    public static ResponsePayloadMsg getSealResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setSealResponse(SealResponseMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a VERSION request message that can be sent by the client.
     */
    public static RequestPayloadMsg getVersionRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setVersionRequest(VersionRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a VERSION response message that can be sent by the server.
     *
     * @param vi   the version information of the server
     */
    public static ResponsePayloadMsg getVersionResponseMsg(VersionInfo vi) {
        final Gson parser = new GsonBuilder().create();
        String payload = parser.toJson(vi);

        return ResponsePayloadMsg.newBuilder()
                .setVersionResponse(VersionResponseMsg.newBuilder()
                        .setJsonPayloadMsg(payload)
                        .build())
                .build();
    }


    /**
     * Returns a HANDSHAKE request message that is sent by the client
     * when initiating a handshake with the server.
     *
     * @param clientId   the client id
     * @param nodeId     the expected id of the server
     */
    public static RequestPayloadMsg getHandshakeRequestMsg(UUID clientId, UUID nodeId) {
        return RequestPayloadMsg.newBuilder()
                .setHandshakeRequest(HandshakeRequestMsg.newBuilder()
                        .setClientId(getUuidMsg(clientId))
                        .setServerId(getUuidMsg(nodeId))
                        .build())
                .build();
    }

    /**
     * Returns a HANDSHAKE response message that is sent by the server
     * when responding to a handshake request.
     *
     * @param nodeId         the server id
     * @param corfuVersion   a string containing corfu version information
     */
    public static ResponsePayloadMsg getHandshakeResponseMsg(UUID nodeId, String corfuVersion) {
        return ResponsePayloadMsg.newBuilder()
                .setHandshakeResponse(HandshakeResponseMsg.newBuilder()
                        .setServerId(getUuidMsg(nodeId))
                        .setCorfuVersion(corfuVersion)
                        .build())
                .build();
    }
}
