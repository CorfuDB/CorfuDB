package org.corfudb.runtime.protocols.sequencers;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerTokenRequestMsg;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerTokenResponseMsg;
import org.corfudb.runtime.protocols.AbstractNettyProtocol;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.NettyRPCChannelInboundHandlerAdapter;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static io.netty.buffer.Unpooled.directBuffer;

/**
 * Created by mwei on 9/15/15.
 */
@Slf4j
public class NettyStreamingSequencerProtocol
        extends AbstractNettyProtocol<NettyStreamingSequencerProtocol.NettyStreamingSequencerHandler>
        implements INewStreamSequencer {


    public static String getProtocolString()
    {
        return "nsss";
    }

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        return new NettyStreamingSequencerProtocol(host, port, options, epoch);
    }

    public NettyStreamingSequencerProtocol(String host, Integer port, Map<String,String> options, long epoch)
    {
        super(host, port, options, epoch, new NettyStreamingSequencerHandler());
    }

    /**
     * Get the next tokens for a particular stream.
     *
     * @param streams   The streams to acquire this token for.
     * @param numTokens The number of tokens to acquire.
     * @return The start of the first token returned.
     */
    @Override
    public CompletableFuture<Long> getNext(Set<UUID> streams, long numTokens) {
        return handler.getToken(streams, numTokens);
    }

    static class NettyStreamingSequencerHandler extends NettyRPCChannelInboundHandlerAdapter {

        //region Handler Interface
        @Override
        public void handleMessage(NettyCorfuMsg message)
        {
            switch (message.getMsgType())
            {
                case PONG:
                    completeRequest(message.getRequestID(), true);
                    break;
                case TOKEN_RES:
                    completeRequest(message.getRequestID(), ((NettyStreamingServerTokenResponseMsg)message).getToken());
                    break;
            }
        }

        public CompletableFuture<Long> getToken(Set<UUID> streamIDs, long numTokens) {
            NettyStreamingServerTokenRequestMsg r =
                    new NettyStreamingServerTokenRequestMsg
                            (streamIDs, numTokens);
            return sendMessageAndGetCompletable(protocol.getEpoch(), r);
        }

        //endregion
    }
}
