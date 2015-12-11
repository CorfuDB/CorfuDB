package org.corfudb.runtime.protocols;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.wireprotocol.NettyCorfuMsg;
import org.corfudb.runtime.wireprotocol.NettyStreamingServerTokenRequestMsg;
import org.corfudb.runtime.wireprotocol.NettyStreamingServerTokenResponseMsg;

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
    public void handleMessage(NettyCorfuMsg msg, ChannelHandlerContext ctx) {
        switch (msg.getMsgType())
        {
            case TOKEN_RES:
                router.completeRequest(msg.getRequestID(), ((NettyStreamingServerTokenResponseMsg)msg).getToken());
                break;
        }
    }

    /** The messages this client should handle. */
    @Getter
    public final Set<NettyCorfuMsg.NettyCorfuMsgType> HandledTypes =
            new ImmutableSet.Builder<NettyCorfuMsg.NettyCorfuMsgType>()
                    .add(NettyCorfuMsg.NettyCorfuMsgType.TOKEN_REQ)
                    .add(NettyCorfuMsg.NettyCorfuMsgType.TOKEN_RES)
                    .build();


    public CompletableFuture<Long> nextToken(Set<UUID> streamIDs, long numTokens)
    {
        return router.sendMessageAndGetCompletable(
                new NettyStreamingServerTokenRequestMsg(streamIDs, numTokens));
    }

}
