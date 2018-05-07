package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;

import lombok.Getter;
import lombok.Setter;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.TokenResponse;


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
    public SequencerClient getClient(long epoch) {
        return new SequencerClient(router, epoch);
    }

    /**
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientMsgHandler msgHandler = new ClientMsgHandler(this)
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
}
