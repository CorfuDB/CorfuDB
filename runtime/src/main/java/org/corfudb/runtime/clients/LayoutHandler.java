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

import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.Layout;
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

    @ResponseHandler(type = PayloadCase.LAYOUT_RESPONSE)
    private static Object handleLayoutResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                               IClientProtobufRouter r) {
        Layout.LayoutResponseMsg layoutResponse = msg.getPayload().getLayoutResponse();
        Common.LayoutMsg layout = layoutResponse.getLayout();

    }
}
