package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.UUID;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.protocols.wireprotocol.LayoutPrepareRequest;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.protocols.wireprotocol.LayoutProposeResponse;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;

import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.Layout.PrepareLayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.LayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.ProposeLayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.CommitLayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.BootstrapLayoutResponseMsg;
import org.corfudb.runtime.proto.Common;

/**
 * A client to the layout server.
 * <p>
 * Used by clients to obtain the layout and to report errors.
 * </p>
 * <p>Created by zlokhandwala on 2/20/18.
 */
public class LayoutHandler implements IClient, IHandler<LayoutClient> {

    @Setter
    @Getter
    IClientRouter router;

    /**
     * The protobuf router to use for the client.
     * For old CorfuMsg, use {@link #router}
     */
    @Getter
    @Setter
    public IClientProtobufRouter protobufRouter;

    @Override
    public LayoutClient getClient(long epoch, UUID clusterID) {
        return new LayoutClient(router, epoch, clusterID);
    }

    /**
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientMsgHandler msgHandler = new ClientMsgHandler(this)
            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * For old CorfuMsg, use {@link #msgHandler}
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientResponseHandler responseHandler = new ClientResponseHandler(this)
            .generateHandlers(MethodHandles.lookup(), this);


    @ClientHandler(type = CorfuMsgType.LAYOUT_RESPONSE)
    private static Object handleLayoutResponse(CorfuMsg msg,
                                               ChannelHandlerContext ctx, IClientRouter r) {
        return ((LayoutMsg) msg).getLayout();
    }

    @ClientHandler(type = CorfuMsgType.LAYOUT_PREPARE_ACK)
    private static Object handleLayoutPrepareAck(CorfuPayloadMsg<LayoutPrepareRequest> msg,
                                                 ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    @ClientHandler(type = CorfuMsgType.LAYOUT_NOBOOTSTRAP)
    private static Object handleNoBootstrap(CorfuMsg msg,
                                            ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        throw new NoBootstrapException();
    }

    @ClientHandler(type = CorfuMsgType.LAYOUT_PREPARE_REJECT)
    private static Object handlePrepareReject(CorfuPayloadMsg<LayoutPrepareResponse> msg,
                                              ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        LayoutPrepareResponse response = msg.getPayload();
        throw new OutrankedException(response.getRank(), response.getLayout());
    }

    @ClientHandler(type = CorfuMsgType.LAYOUT_PROPOSE_REJECT)
    private static Object handleProposeReject(CorfuPayloadMsg<LayoutProposeResponse> msg,
                                              ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        LayoutProposeResponse response = msg.getPayload();
        throw new OutrankedException(response.getRank());
    }

    @ClientHandler(type = CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP)
    private static Object handleAlreadyBootstrap(CorfuMsg msg,
                                                 ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        throw new AlreadyBootstrappedException();
    }

    // Protobuf region

    @ResponseHandler(type = PayloadCase.LAYOUT_RESPONSE)
    private static Object handleLayoutResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                               IClientProtobufRouter r) {
        LayoutResponseMsg layoutResponse = msg.getPayload().getLayoutResponse();
        Common.LayoutMsg layout = layoutResponse.getLayout();

        return Layout.fromJSONString(layout.getLayoutJson());
    }

    @ResponseHandler(type = PayloadCase.PREPARE_LAYOUT_RESPONSE)
    private static Object handlePrepareLayoutResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                      IClientProtobufRouter r) {
        PrepareLayoutResponseMsg prepareLayoutMsg = msg.getPayload().getPrepareLayoutResponse();
        PrepareLayoutResponseMsg.Type type = prepareLayoutMsg.getRespType();
        long rank = prepareLayoutMsg.getRank();
        Layout layout = Layout.fromJSONString(prepareLayoutMsg.getLayout().getLayoutJson());

        switch (type) {
            case ACK: return new LayoutPrepareResponse(rank, layout);
            case REJECT: throw new OutrankedException(rank, layout);
            // TODO for INVALID
            default: throw new UnsupportedOperationException("Response handler not provided");
        }
    }

    @ResponseHandler(type = PayloadCase.PROPOSE_LAYOUT_RESPONSE)
    private static Object handleProposeLayoutResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                      IClientProtobufRouter r) {
        ProposeLayoutResponseMsg proposeLayoutMsg = msg.getPayload().getProposeLayoutResponse();
        ProposeLayoutResponseMsg.Type type = proposeLayoutMsg.getRespType();
        long rank = proposeLayoutMsg.getRank();

        switch (type) {
            case ACK: return true;
            case REJECT: throw new OutrankedException(rank);
            // TODO for INVALID
            default: throw new UnsupportedOperationException("Response handler not provided");
        }
    }

    @ResponseHandler(type = PayloadCase.COMMIT_LAYOUT_RESPONSE)
    private static Object handleCommitLayoutResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                     IClientProtobufRouter r) {
        CommitLayoutResponseMsg commitLayoutMsg = msg.getPayload().getCommitLayoutResponse();
        CommitLayoutResponseMsg.Type type = commitLayoutMsg.getRespType();

        switch (type) {
            case ACK: return true;
            case NACK: return false;
            // TODO for INVALID
            default: throw new UnsupportedOperationException("Response handler not provided");
        }
    }

    @ResponseHandler(type = PayloadCase.BOOTSTRAP_LAYOUT_RESPONSE)
    private static Object handleBootstrapLayoutResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                        IClientProtobufRouter r) {
        BootstrapLayoutResponseMsg bootstrapLayoutMsg =  msg.getPayload().getBootstrapLayoutResponse();
        BootstrapLayoutResponseMsg.Type type = bootstrapLayoutMsg.getRespType();

        switch (type) {
            case ACK: return true;
            case NACK: return false;
            // TODO for INVALID
            default: throw new UnsupportedOperationException("Response handler not provided");
        }
    }

    // End region
}
