package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.UUID;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;

import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.Layout.PrepareLayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.LayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.ProposeLayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.CommitLayoutResponseMsg;
import org.corfudb.runtime.proto.service.Layout.BootstrapLayoutResponseMsg;
import org.corfudb.runtime.proto.RpcCommon;

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

    @Override
    public LayoutClient getClient(long epoch, UUID clusterID) {
        return new LayoutClient(router, epoch, clusterID);
    }

    /**
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientResponseHandler responseHandler = new ClientResponseHandler(this)
            .generateHandlers(MethodHandles.lookup(), this)
            .generateErrorHandlers(MethodHandles.lookup(), this);

    /**
     * Handle a layout response from the server.
     *
     * @param msg The layout response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return {@link Layout} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.LAYOUT_RESPONSE)
    private static Object handleLayoutResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                               IClientRouter r) {
        LayoutResponseMsg layoutResponse = msg.getPayload().getLayoutResponse();
        RpcCommon.LayoutMsg layoutMsg = layoutResponse.getLayout();

        return CorfuProtocolCommon.getLayout(layoutMsg);
    }

    /**
     * Handle a prepare layout response from the server.
     *
     * @param msg The prepare layout response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return {@link LayoutPrepareResponse} if ACK, throw an {@link OutrankedException} if REJECT.
     */
    @ResponseHandler(type = PayloadCase.PREPARE_LAYOUT_RESPONSE)
    private static Object handlePrepareLayoutResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                      IClientRouter r) {
        PrepareLayoutResponseMsg prepareLayoutMsg = msg.getPayload().getPrepareLayoutResponse();
        boolean prepared = prepareLayoutMsg.getPrepared();
        long rank = prepareLayoutMsg.getRank();
        Layout layout = CorfuProtocolCommon.getLayout(prepareLayoutMsg.getLayout());

        if (!prepared) {
            throw new OutrankedException(rank, layout);
        }
        return new LayoutPrepareResponse(rank, layout);
    }

    /**
     * Handle a propose layout response from the server.
     *
     * @param msg The propose layout response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return True if ACK, throw an {@link OutrankedException} if REJECT.
     */
    @ResponseHandler(type = PayloadCase.PROPOSE_LAYOUT_RESPONSE)
    private static Object handleProposeLayoutResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                      IClientRouter r) {
        ProposeLayoutResponseMsg proposeLayoutMsg = msg.getPayload().getProposeLayoutResponse();
        boolean proposed = proposeLayoutMsg.getProposed();
        long rank = proposeLayoutMsg.getRank();

        if (!proposed) {
            throw new OutrankedException(rank);
        }
        return true;
    }

    /**
     * Handle a propose layout response from the server.
     *
     * @param msg The propose layout response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return True if ACK, throw an {@link OutrankedException} if REJECT.
     */
    @ResponseHandler(type = PayloadCase.COMMIT_LAYOUT_RESPONSE)
    private static Object handleCommitLayoutResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                     IClientRouter r) {
        CommitLayoutResponseMsg commitLayoutMsg = msg.getPayload().getCommitLayoutResponse();

        return commitLayoutMsg.getCommitted();
    }

    /**
     * Handle a bootstrap layout response from the server.
     *
     * @param msg The propose layout response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return True if ACK, false if NACK.
     */
    @ResponseHandler(type = PayloadCase.BOOTSTRAP_LAYOUT_RESPONSE)
    private static Object handleBootstrapLayoutResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                        IClientRouter r) {
        BootstrapLayoutResponseMsg bootstrapLayoutMsg =  msg.getPayload().getBootstrapLayoutResponse();

        return bootstrapLayoutMsg.getBootstrapped();
    }
}
