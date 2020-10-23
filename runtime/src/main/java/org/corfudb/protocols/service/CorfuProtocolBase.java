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

@Slf4j
public class CorfuProtocolBase {
    public static RequestPayloadMsg getPingRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setPingRequest(PingRequestMsg.getDefaultInstance())
                .build();
    }

    public static ResponsePayloadMsg getPingResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setPingResponse(PingResponseMsg.getDefaultInstance())
                .build();
    }

    public static RequestPayloadMsg getRestartRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setRestartRequest(RestartRequestMsg.getDefaultInstance())
                .build();
    }

    public static ResponsePayloadMsg getRestartResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setRestartResponse(RestartResponseMsg.getDefaultInstance())
                .build();
    }

    public static RequestPayloadMsg getResetRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setResetRequest(ResetRequestMsg.getDefaultInstance())
                .build();
    }

    public static ResponsePayloadMsg getResetResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setResetResponse(ResetResponseMsg.getDefaultInstance())
                .build();
    }

    public static RequestPayloadMsg getSealRequestMsg(long epoch) {
        return RequestPayloadMsg.newBuilder()
                .setSealRequest(SealRequestMsg.newBuilder()
                        .setEpoch(epoch)
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getSealResponseMsg() {
        return ResponsePayloadMsg.newBuilder()
                .setSealResponse(SealResponseMsg.getDefaultInstance())
                .build();
    }

    public static RequestPayloadMsg getVersionRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setVersionRequest(VersionRequestMsg.getDefaultInstance())
                .build();
    }

    public static ResponsePayloadMsg getVersionResponseMsg(VersionInfo vi) {
        final Gson parser = new GsonBuilder().create();
        String payload = parser.toJson(vi);

        return ResponsePayloadMsg.newBuilder()
                .setVersionResponse(VersionResponseMsg.newBuilder()
                        .setJsonPayloadMsg(payload)
                        .build())
                .build();
    }

    public static VersionInfo getVersionInfo(VersionResponseMsg msg) {
        String jsonPayloadMsg = msg.getJsonPayloadMsg();
        final Gson parser = new Gson();

        return parser.fromJson(jsonPayloadMsg, VersionInfo.class);
    }

    public static RequestPayloadMsg getHandshakeRequestMsg(UUID clientId, UUID nodeId) {
        return RequestPayloadMsg.newBuilder()
                .setHandshakeRequest(HandshakeRequestMsg.newBuilder()
                        .setClientId(getUuidMsg(clientId))
                        .setServerId(getUuidMsg(nodeId))
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getHandshakeResponseMsg(UUID nodeId, String corfuVersion) {
        return ResponsePayloadMsg.newBuilder()
                .setHandshakeResponse(HandshakeResponseMsg.newBuilder()
                        .setServerId(getUuidMsg(nodeId))
                        .setCorfuVersion(corfuVersion)
                        .build())
                .build();
    }
}
