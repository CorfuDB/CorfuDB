package org.corfudb.protocols.service;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.proto.Common.UuidMsg;
import org.corfudb.runtime.proto.ServerErrors.ServerErrorMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage.ProtocolVersion;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;

import java.util.UUID;

import static org.corfudb.protocols.CorfuProtocolCommon.*;

@Slf4j
public class CorfuProtocolMessage {
    public static final ProtocolVersion CURRENT_VERSION = ProtocolVersion.V0;

    public static HeaderMsg getHeaderMsg(long requestId, PriorityLevel priority, long epoch, UuidMsg clusterId,
                                         UuidMsg clientId, boolean ignoreClusterId, boolean ignoreEpoch) {
        return HeaderMsg.newBuilder()
                .setVersion(CURRENT_VERSION)
                .setRequestId(requestId)
                .setPriority(priority)
                .setEpoch(epoch)
                .setClusterId(clusterId)
                .setClientId(clientId)
                .setIgnoreClusterId(ignoreClusterId)
                .setIgnoreEpoch(ignoreEpoch)
                .build();
    }

    public static HeaderMsg getHeaderMsg(long requestId, PriorityLevel priority, long epoch, UUID clusterId,
                                         UUID clientId, boolean ignoreClusterId, boolean ignoreEpoch) {
        return getHeaderMsg(requestId, priority, epoch, getUuidMsg(clusterId),
                getUuidMsg(clientId), ignoreClusterId, ignoreEpoch);
    }

    public static HeaderMsg getHeaderMsg(HeaderMsg header, boolean ignoreClusterId, boolean ignoreEpoch) {
        return getHeaderMsg(header.getRequestId(),
                header.getPriority(),
                header.getEpoch(),
                header.getClusterId(),
                header.getClientId(),
                ignoreClusterId,
                ignoreEpoch);
    }

    public static HeaderMsg getHighPriorityHeaderMsg(HeaderMsg header) {
        return getHeaderMsg(header.getRequestId(),
                PriorityLevel.HIGH,
                header.getEpoch(),
                header.getClusterId(),
                header.getClientId(),
                header.getIgnoreClusterId(),
                header.getIgnoreEpoch());
    }

    public static RequestMsg getRequestMsg(HeaderMsg header, RequestPayloadMsg request) {
        return RequestMsg.newBuilder()
                .setHeader(header)
                .setPayload(request)
                .build();
    }

    public static ResponseMsg getResponseMsg(HeaderMsg header, ResponsePayloadMsg response) {
        return ResponseMsg.newBuilder()
                .setHeader(header)
                .setPayload(response)
                .build();
    }

    public static ResponseMsg getResponseMsg(HeaderMsg header, ServerErrorMsg error) {
        return ResponseMsg.newBuilder()
                .setHeader(header)
                .setPayload(ResponsePayloadMsg.newBuilder()
                        .setServerError(error)
                        .build())
                .build();
    }
}
