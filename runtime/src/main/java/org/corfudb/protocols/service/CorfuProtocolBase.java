package org.corfudb.protocols.service;

import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
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
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;

/**
 * This class provides methods for creating the Protobuf objects defined
 * in base.proto. These provide the interface for performing the RPCs
 * handled by the Base server, as well as the handshake.
 */
@Slf4j
public final class CorfuProtocolBase {
    // Prevent class from being instantiated
    private CorfuProtocolBase() {}

    /**
     * Returns a PING request message that can be sent by the client.
     *
     * @return   a RequestPayloadMsg containing a PING request
     */
    public static RequestPayloadMsg getPingRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setPingRequest(PingRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a PING response message that can be sent by the server.
     *
     * @return   a ResponsePayloadMsg containing a PING response
     */
    public static ResponsePayloadMsg getPingResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setPingResponse(PingResponseMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a RESTART request message that can be sent by the client.
     *
     * @return   a RequestPayloadMsg containing a RESTART request
     */
    public static RequestPayloadMsg getRestartRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setRestartRequest(RestartRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a RESTART response message that can be sent by the server.
     *
     * @return   a ResponsePayloadMsg containing a RESTART response
     */
    public static ResponsePayloadMsg getRestartResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setRestartResponse(RestartResponseMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a RESET request message that can be sent by the client.
     *
     * @return   a RequestPayloadMsg containing a RESET request
     */
    public static RequestPayloadMsg getResetRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setResetRequest(ResetRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a RESET response message that can be sent by the server.
     *
     * @return   a ResponsePayloadMsg containing a RESET response
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
     * @return        a RequestPayloadMsg containing a SEAL request
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
     *
     * @return   a ResponsePayloadMsg containing a SEAL response
     */
    public static ResponsePayloadMsg getSealResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setSealResponse(SealResponseMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a HANDSHAKE request message that is sent by the client
     * when initiating a handshake with the server.
     *
     * @param clientId   the client id
     * @param nodeId     the expected id of the server
     * @return           a RequestPayloadMsg containing a HANDSHAKE request
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
     * @return               a ResponsePayloadMsg containing a HANDSHAKE response
     */
    public static ResponsePayloadMsg getHandshakeResponseMsg(UUID nodeId) {
        return ResponsePayloadMsg.newBuilder()
                .setHandshakeResponse(HandshakeResponseMsg.newBuilder()
                        .setServerId(getUuidMsg(nodeId))
                        .build())
                .build();
    }
}
