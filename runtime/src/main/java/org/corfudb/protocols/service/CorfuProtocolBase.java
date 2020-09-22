package org.corfudb.protocols.service;

import lombok.extern.slf4j.Slf4j;
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

    //TODO: Complete handshake (authenticate) and version info message types
}
