package org.corfudb.protocols.service;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;
import org.corfudb.runtime.proto.service.Layout.BootstrapLayoutRequestMsg;
import org.corfudb.runtime.proto.service.Layout.BootstrapLayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.CommitLayoutRequestMsg;
import org.corfudb.runtime.proto.service.Layout.CommitLayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.LayoutRequestMsg;
import org.corfudb.runtime.proto.service.Layout.LayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.PrepareLayoutRequestMsg;
import org.corfudb.runtime.proto.service.Layout.PrepareLayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.ProposeLayoutRequestMsg;
import org.corfudb.runtime.proto.service.Layout.ProposeLayoutResponseMsg;
import org.corfudb.runtime.view.Layout;

import static org.corfudb.protocols.CorfuProtocolCommon.*;

@Slf4j
public class CorfuProtocolLayout {
    public static RequestPayloadMsg getLayoutRequestMsg(long epoch) {
        return RequestPayloadMsg.newBuilder()
                .setLayoutRequest(LayoutRequestMsg.newBuilder()
                        .setEpoch(epoch)
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getLayoutResponseMsg(Layout layout) {
        return ResponsePayloadMsg.newBuilder()
                .setLayoutResponse(LayoutResponseMsg.newBuilder()
                        .setLayout(getLayoutMsg(layout))
                        .build())
                .build();
    }

    public static RequestPayloadMsg getPrepareLayoutRequestMsg(long epoch, long rank) {
        return RequestPayloadMsg.newBuilder()
                .setPrepareLayoutRequest(PrepareLayoutRequestMsg.newBuilder()
                        .setEpoch(epoch)
                        .setRank(rank)
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getPrepareLayoutResponseMsg(PrepareLayoutResponseMsg.Type type,
                                                                 long rank, Layout layout) {
        return ResponsePayloadMsg.newBuilder()
                .setPrepareLayoutResponse(PrepareLayoutResponseMsg.newBuilder()
                        .setRespType(type)
                        .setRank(rank)
                        .setLayout(getLayoutMsg(layout))
                        .build())
                .build();
    }

    public static LayoutPrepareResponse getLayoutPrepareResponse(PrepareLayoutResponseMsg msg) {
        return new LayoutPrepareResponse(msg.getRank(), getLayout(msg.getLayout()));
    }

    public static RequestPayloadMsg getProposeLayoutRequestMsg(long epoch, long rank, Layout layout) {
        return RequestPayloadMsg.newBuilder()
                .setProposeLayoutRequest(ProposeLayoutRequestMsg.newBuilder()
                        .setEpoch(epoch)
                        .setRank(rank)
                        .setLayout(getLayoutMsg(layout))
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getProposeLayoutResponseMsg(ProposeLayoutResponseMsg.Type type, long rank) {
        return ResponsePayloadMsg.newBuilder()
                .setProposeLayoutResponse(ProposeLayoutResponseMsg.newBuilder()
                        .setRespType(type)
                        .setRank(rank)
                        .build())
                .build();
    }

    public static RequestPayloadMsg getCommitLayoutRequestMsg(boolean forced, long epoch, Layout layout) {
        return RequestPayloadMsg.newBuilder()
                .setCommitLayoutRequest(CommitLayoutRequestMsg.newBuilder()
                        .setForced(forced)
                        .setEpoch(epoch)
                        .setLayout(getLayoutMsg(layout))
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getCommitLayoutResponseMsg(CommitLayoutResponseMsg.Type type) {
        return ResponsePayloadMsg.newBuilder()
                .setCommitLayoutResponse(CommitLayoutResponseMsg.newBuilder()
                        .setRespType(type)
                        .build())
                .build();
    }

    public static RequestPayloadMsg getBootstrapLayoutRequestMsg(Layout layout) {
        return RequestPayloadMsg.newBuilder()
                .setBootstrapLayoutRequest(BootstrapLayoutRequestMsg.newBuilder()
                        .setLayout(getLayoutMsg(layout))
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getBootstrapLayoutResponseMsg(BootstrapLayoutResponseMsg.Type type) {
        return ResponsePayloadMsg.newBuilder()
                .setBootstrapLayoutResponse(BootstrapLayoutResponseMsg.newBuilder()
                        .setRespType(type)
                        .build())
                .build();
    }
}
