package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.runtime.proto.RpcCommon.SequencerMetricsMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.Sequencer.SequencerMetricsResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.TokenResponseMsg;

import java.lang.invoke.MethodHandles;
import java.util.UUID;

import static org.corfudb.protocols.CorfuProtocolCommon.getSequencerMetrics;
import static org.corfudb.protocols.CorfuProtocolCommon.getStreamsAddressResponse;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getTokenResponse;

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

    @Override
    public SequencerClient getClient(long epoch, UUID clusterID) {
        return new SequencerClient(router, epoch, clusterID);
    }

    /**
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientResponseHandler responseHandler = new ClientResponseHandler(this)
            .generateHandlers(MethodHandles.lookup(), this)
            .generateErrorHandlers(MethodHandles.lookup(), this);

    /**
     * Handle a token response from the server.
     *
     * @param msg      The token response message.
     * @param ctx      The context the message was sent under.
     * @param router   A reference to the router.
     * @return {@link TokenResponseMsg} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.TOKEN_RESPONSE)
    private static Object handleTokenResponse(ResponseMsg msg,
                                              ChannelHandlerContext ctx,
                                              IClientRouter router) {
        TokenResponseMsg responseMsg = msg.getPayload().getTokenResponse();

        return getTokenResponse(responseMsg);
    }

    /**
     * Handle a bootstrap sequencer response from the server.
     *
     * @param msg      The bootstrap sequencer response message.
     * @param ctx      The context the message was sent under.
     * @param router   A reference to the router.
     * @return True if ACK, false if NACK.
     */
    @ResponseHandler(type = PayloadCase.BOOTSTRAP_SEQUENCER_RESPONSE)
    private static Object handleBootstrapSequencerResponse(ResponseMsg msg,
                                                           ChannelHandlerContext ctx,
                                                           IClientRouter router) {
        return  msg.getPayload().getBootstrapSequencerResponse().getIsBootstrapped();
    }

    /**
     * Handle a sequencer trim response from the server.
     *
     * @param msg      The sequencer trim response message.
     * @param ctx      The context the message was sent under.
     * @param router   A reference to the router.
     * @return Always True, since the sequencer trim was successful.
     */
    @ResponseHandler(type = PayloadCase.SEQUENCER_TRIM_RESPONSE)
    private static Object handleSequencerTrimResponse(ResponseMsg msg,
                                                      ChannelHandlerContext ctx,
                                                      IClientRouter router) {
        return true;
    }

    /**
     * Handle a sequencer metrics response from the server.
     *
     * @param msg      The sequencer metrics response message.
     * @param ctx      The context the message was sent under.
     * @param router   A reference to the router.
     * @return {@link SequencerMetrics} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.SEQUENCER_METRICS_RESPONSE)
    private static Object handleSequencerMetricsResponse(ResponseMsg msg,
                                                         ChannelHandlerContext ctx,
                                                         IClientRouter router) {
        SequencerMetricsResponseMsg responseMsg = msg.getPayload().getSequencerMetricsResponse();
        SequencerMetricsMsg sequencerMetricsMsg = responseMsg.getSequencerMetrics();

        return getSequencerMetrics(sequencerMetricsMsg);
    }

    /**
     * Handle a streams address response from the server.
     *
     * @param msg      The streams address response message.
     * @param ctx      The context the message was sent under.
     * @param router   A reference to the router.
     * @return {@link StreamsAddressResponse} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.STREAMS_ADDRESS_RESPONSE)
    private static Object handleStreamsAddressResponse(ResponseMsg msg,
                                                       ChannelHandlerContext ctx,
                                                       IClientRouter router) {
        return getStreamsAddressResponse(msg.getPayload().getStreamsAddressResponse().getLogTail(),
                msg.getPayload().getStreamsAddressResponse().getEpoch(),
                msg.getPayload().getStreamsAddressResponse().getAddressMapList());
    }
}
