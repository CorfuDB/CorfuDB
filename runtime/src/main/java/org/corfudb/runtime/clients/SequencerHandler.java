package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.UUID;

import lombok.Getter;
import lombok.Setter;

import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.service.CorfuProtocolSequencer;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TokenResponse;

import org.corfudb.runtime.proto.Common.SequencerMetricsMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.TokenResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.BootstrapSequencerResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.SequencerMetricsResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.StreamsAddressResponseMsg;


/**
 * A sequencer handler client.
 * This client handles the token responses from the sequencer server.
 *
 * <p>Created by zlokhandwala on 2/20/18.
 */
public class SequencerHandler implements IClient, IHandler<SequencerClient> {


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
    public SequencerClient getClient(long epoch, UUID clusterID) {
        return new SequencerClient(router, epoch, clusterID);
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

    @ClientHandler(type = CorfuMsgType.SEQUENCER_METRICS_RESPONSE)
    private static Object handleMetricsResponse(CorfuPayloadMsg<SequencerMetrics> msg,
                                                ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    @ClientHandler(type = CorfuMsgType.TOKEN_RES)
    private static Object handleTokenResponse(CorfuPayloadMsg<TokenResponse> msg,
                                              ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    @ClientHandler(type = CorfuMsgType.STREAMS_ADDRESS_RESPONSE)
    private static Object handleStreamAddressesResponse(CorfuPayloadMsg<StreamsAddressResponse> msg,
                                              ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    // Protobuf region

    /**
     * Handle a token response from the server.
     *
     * @param msg The token response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return {@link TokenResponse} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.TOKEN_RESPONSE)
    private static Object handleTokenResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                              IClientProtobufRouter r) {
        TokenResponseMsg responseMsg = msg.getPayload().getTokenResponse();

        return CorfuProtocolSequencer.getTokenResponse(responseMsg);
    }

    /**
     * Handle a bootstrap sequencer response from the server.
     *
     * @param msg The bootstrap sequencer response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return True if ACK, false if NACK.
     */
    @ResponseHandler(type = PayloadCase.BOOTSTRAP_SEQUENCER_RESPONSE)
    private static Object handleBootstrapSequencerResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                           IClientProtobufRouter r) {
        BootstrapSequencerResponseMsg responseMsg = msg.getPayload().getBootstrapSequencerResponse();
        BootstrapSequencerResponseMsg.Type type = responseMsg.getRespType();

        switch (type) {
            case ACK:   return true;
            case NACK:  return false;
            // TODO INVALID
            default:    throw new UnsupportedOperationException("Response handler not provided");
        }
    }

    /**
     * Handle a sequencer trim response from the server.
     *
     * @param msg The sequencer trim response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return Always True, since the sequencer trim was successful.
     */
    @ResponseHandler(type = PayloadCase.SEQUENCER_TRIM_RESPONSE)
    private static Object handleSequencerTrimResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                      IClientProtobufRouter r) {
        return true;
    }

    /**
     * Handle a sequencer metrics response from the server.
     *
     * @param msg The sequencer metrics response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return {@link SequencerMetrics} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.SEQUENCER_METRICS_RESPONSE)
    private static Object handleSequencerMetricsResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                      IClientProtobufRouter r) {
        SequencerMetricsResponseMsg responseMsg = msg.getPayload().getSequencerMetricsResponse();
        SequencerMetricsMsg sequencerMetricsMsg = responseMsg.getSequencerMetrics();

        return CorfuProtocolCommon.getSequencerMetrics(sequencerMetricsMsg);
    }

    /**
     * Handle a streams address response from the server.
     *
     * @param msg The streams address response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return {@link StreamsAddressResponse} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.STREAMS_ADDRESS_RESPONSE)
    private static Object handleStreamsAddressResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                       IClientProtobufRouter r) {
        StreamsAddressResponseMsg responseMsg = msg.getPayload().getStreamsAddressResponse();

        return CorfuProtocolCommon.getStreamsAddressResponse(responseMsg.getLogTail(), responseMsg.getAddressMapList());
    }

    // End region
}
