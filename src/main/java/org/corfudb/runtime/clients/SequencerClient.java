package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelHandlerContext;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.*;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A sequencer client.
 * <p>
 * This client allows the client to obtain sequence numbers from a sequencer.
 * <p>
 * Created by mwei on 12/10/15.
 */
public class SequencerClient implements IClient {

    /**
     * The messages this client should handle.
     */
    @Getter
    public final Set<CorfuMsgType> HandledTypes =
            new ImmutableSet.Builder<CorfuMsgType>()
                    .add(CorfuMsgType.TOKEN_REQ)
                    .add(CorfuMsgType.TOKEN_RES)
                    .build();
    @Setter
    @Getter
    IClientRouter router;

    /**
     * Handle a incoming message on the channel
     *
     * @param msg The incoming message
     * @param ctx The channel handler context
     */
    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        switch (msg.getMsgType()) {
            case TOKEN_RES:
               CorfuPayloadMsg<TokenResponse> tmsg = ((CorfuPayloadMsg<TokenResponse>) msg);
                router.completeRequest(msg.getRequestID(), tmsg.getPayload());
                break;
        }
    }

    public CompletableFuture<TokenResponse> nextToken(Set<UUID> streamIDs, long numTokens) {
        return router.sendMessageAndGetCompletable(
                new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ, new TokenRequest(numTokens, streamIDs)));
    }

}
