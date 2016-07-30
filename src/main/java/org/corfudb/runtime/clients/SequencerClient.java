package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelHandlerContext;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.TokenRequestMsg;
import org.corfudb.protocols.wireprotocol.TokenResponseMsg;

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
    public final Set<CorfuMsg.CorfuMsgType> HandledTypes =
            new ImmutableSet.Builder<CorfuMsg.CorfuMsgType>()
                    .add(CorfuMsg.CorfuMsgType.TOKEN_REQ)
                    .add(CorfuMsg.CorfuMsgType.TOKEN_RES)
                    .build();
    @Setter
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
                TokenResponseMsg tmsg = ((TokenResponseMsg) msg);
                router.completeRequest(msg.getRequestID(),
                        new TokenResponse(tmsg.getToken(), tmsg.getBackpointerMap()));
                break;
        }
    }

    public CompletableFuture<TokenResponse> nextToken(Set<UUID> streamIDs, long numTokens) {
        return router.sendMessageAndGetCompletable(
                new TokenRequestMsg(streamIDs, numTokens));
    }

    @Data
    public class TokenResponse {
        public final Long token;
        public final Map<UUID, Long> backpointerMap;
    }

}
