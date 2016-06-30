package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuSetEpochMsg;
import org.corfudb.runtime.exceptions.WrongEpochException;

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
     * @param router   The router that routed the incoming message.
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
            case SET_EPOCH:
                {
                    CorfuSetEpochMsg csem = (CorfuSetEpochMsg) msg;
                    if (csem.getNewEpoch() >= router.getEpoch())
                    {
                        log.info("Received SET_EPOCH, moving to new epoch {}", csem.getNewEpoch());
                        router.setEpoch(csem.getNewEpoch());
                        router.sendResponseToServer(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
                    }
                    else
                    {
                        log.debug("Rejected SET_EPOCH currrent={}, requested={}",
                                router.getEpoch(), csem.getNewEpoch());
                        router.sendResponseToServer(ctx, msg, new CorfuSetEpochMsg(CorfuMsg.CorfuMsgType.WRONG_EPOCH,
                                router.getEpoch()));
                    }
                }
                break;
            case WRONG_EPOCH:
            {
                CorfuSetEpochMsg csem = (CorfuSetEpochMsg) msg;
                router.completeExceptionally(msg.getRequestID(), new WrongEpochException(csem.getNewEpoch()));
            }
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
                    .add(CorfuMsg.CorfuMsgType.SET_EPOCH)
                    .add(CorfuMsg.CorfuMsgType.WRONG_EPOCH)
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

    public CompletableFuture<Boolean> setRemoteEpoch(long newEpoch) {
        // Set our own epoch to this epoch.
        router.setEpoch(newEpoch);
        return router.sendMessageAndGetCompletable(
                new CorfuSetEpochMsg(CorfuMsg.CorfuMsgType.SET_EPOCH, newEpoch));
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
