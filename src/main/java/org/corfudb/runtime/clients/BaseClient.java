package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** This is a base client which processes basic messages.
 * It mainly handles PINGs, as well as the ACK/NACKs defined by
 * the Corfu protocol.
 *
 * Created by mwei on 12/9/15.
 */
@Slf4j
public class BaseClient implements IClient {

    /** The router to use for the client. */
    @Getter
    @Setter
    public IClientRouter router;

    /**
     * Handle a incoming message on the channel
     *
     * @param msg The incoming message
     * @param ctx The channel handler context
     * @param r   The router that routed the incoming message.
     */
    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        switch (msg.getMsgType())
        {
            case PONG:
                    router.completeRequest(msg.getRequestID(), true);
                break;
            case PING:
                    router.sendResponseToServer(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.PONG));
                break;
            case ACK:
                    router.completeRequest(msg.getRequestID(), true);
                break;
            case NACK:
                router.completeRequest(msg.getRequestID(), false);
                break;
        }
    }

    /** The messages this client should handle. */
    @Getter
    public final Set<CorfuMsg.CorfuMsgType> HandledTypes =
            new ImmutableSet.Builder<CorfuMsg.CorfuMsgType>()
                    .add(CorfuMsg.CorfuMsgType.PING)
                    .add(CorfuMsg.CorfuMsgType.PONG)
                    .add(CorfuMsg.CorfuMsgType.ACK)
                    .add(CorfuMsg.CorfuMsgType.NACK)
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
                new CorfuMsg(CorfuMsg.CorfuMsgType.PING));
    }
}
