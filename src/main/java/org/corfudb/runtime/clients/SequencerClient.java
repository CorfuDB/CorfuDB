package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.TokenRequestMsg;
import org.corfudb.protocols.wireprotocol.TokenResponseMsg;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 12/10/15.
 */
public class SequencerClient implements INettyClient {
    @Setter
    NettyClientRouter router;

    /**
     * Handle a incoming message on the channel
     *
     * @param msg The incoming message
     * @param ctx The channel handler context
     */
    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        switch (msg.getMsgType())
        {
            case TOKEN_RES:
                router.completeRequest(msg.getRequestID(), ((TokenResponseMsg)msg).getToken());
                break;
        }
    }

    /** The messages this client should handle. */
    @Getter
    public final Set<CorfuMsg.NettyCorfuMsgType> HandledTypes =
            new ImmutableSet.Builder<CorfuMsg.NettyCorfuMsgType>()
                    .add(CorfuMsg.NettyCorfuMsgType.TOKEN_REQ)
                    .add(CorfuMsg.NettyCorfuMsgType.TOKEN_RES)
                    .build();


    public CompletableFuture<Long> nextToken(Set<UUID> streamIDs, long numTokens)
    {
        return router.sendMessageAndGetCompletable(
                new TokenRequestMsg(streamIDs, numTokens));
    }

}
