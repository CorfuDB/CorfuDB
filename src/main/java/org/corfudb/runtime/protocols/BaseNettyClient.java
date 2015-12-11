package org.corfudb.runtime.protocols;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 12/9/15.
 */
@Slf4j
public class BaseNettyClient implements INettyClient {

    /** The router to use for the client. */
    @Getter
    @Setter
    public NettyClientRouter router;

    /**
     * Handle a incoming message on the channel
     *
     * @param msg The incoming message
     * @param ctx The channel handler context
     * @param r   The router that routed the incoming message.
     */
    @Override
    public void handleMessage(NettyCorfuMsg msg, ChannelHandlerContext ctx) {
        switch (msg.getMsgType())
        {
            case PONG:
                    router.completeRequest(msg.getRequestID(), true);
                break;
            case PING:
                    router.sendResponse(ctx, msg, new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.PONG));
                break;
        }
    }

    /** The messages this client should handle. */
    @Getter
    public final Set<NettyCorfuMsg.NettyCorfuMsgType> HandledTypes =
            new ImmutableSet.Builder<NettyCorfuMsg.NettyCorfuMsgType>()
                    .add(NettyCorfuMsg.NettyCorfuMsgType.PING)
                    .add(NettyCorfuMsg.NettyCorfuMsgType.PONG)
                    .add(NettyCorfuMsg.NettyCorfuMsgType.ACK)
                    .build();

    /** Ping the endpoint, synchronously.
     *
     * @return True, if the endpoint was reachable, false otherwise.
     */
    public boolean pingSync() {
        try {
            return ping().get();
        } catch (Exception e)
        {
            log.trace("Ping failed due to exception", e);
            return false;
        }
    }

    /** Ping the endpoint, asynchronously.
     *
     * @return A completable future which will be completed with True if
     *          the endpoint is reachable, otherwise False or exceptional completion.
     */
    public CompletableFuture<Boolean> ping() {
        return router.sendMessageAndGetCompletable(
                new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.PING));
    }
}
