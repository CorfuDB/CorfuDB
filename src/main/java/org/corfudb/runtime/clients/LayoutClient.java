package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelHandlerContext;
import lombok.*;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.protocols.wireprotocol.LayoutRankMsg;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A client to the layout server.
 * <p>
 * In addition to being used by clients to obtain the layout and to report errors,
 * The layout client is also used by layout servers to initiate a Paxos-based protocol
 * for determining the next layout.
 * </p>
 * Created by mwei on 12/9/15.
 */
public class LayoutClient implements IClient {

    /**
     * The messages this client should handle.
     */
    @Getter
    public final Set<CorfuMsg.CorfuMsgType> HandledTypes =
            new ImmutableSet.Builder<CorfuMsg.CorfuMsgType>()
                    .add(CorfuMsg.CorfuMsgType.LAYOUT_REQUEST)
                    .add(CorfuMsg.CorfuMsgType.LAYOUT_RESPONSE)
                    .add(CorfuMsg.CorfuMsgType.LAYOUT_PREPARE)
                    .add(CorfuMsg.CorfuMsgType.LAYOUT_BOOTSTRAP)
                    .add(CorfuMsg.CorfuMsgType.LAYOUT_NOBOOTSTRAP)
                    .add(CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_ACK)
                    .add(CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_REJECT)
                    .add(CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT)
                    .add(CorfuMsg.CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP)
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
            case LAYOUT_RESPONSE:
                router.completeRequest(msg.getRequestID(), ((LayoutMsg) msg).getLayout());
                break;
            case LAYOUT_NOBOOTSTRAP:
                router.completeExceptionally(msg.getRequestID(),
                        new NoBootstrapException());
                break;
            case LAYOUT_PREPARE_ACK:
                router.completeRequest(msg.getRequestID(), new LayoutPrepareResponse(true, ((LayoutRankMsg)msg ).getLayout()));
                break;
            case LAYOUT_PREPARE_REJECT:
                router.completeExceptionally(msg.getRequestID(),
                        new OutrankedException(((LayoutRankMsg) msg).getRank()));
                break;
            case LAYOUT_PROPOSE_REJECT:
                router.completeExceptionally(msg.getRequestID(),
                        new OutrankedException(((LayoutRankMsg) msg).getRank()));
                break;
            case LAYOUT_ALREADY_BOOTSTRAP:
                router.completeExceptionally(msg.getRequestID(),
                        new AlreadyBootstrappedException());
                break;
        }
    }



    /**
     * Retrieves the layout from the endpoint, asynchronously.
     * @return A future which will be completed with the current layout.
     */
    public CompletableFuture<Layout> getLayout() {
        return router.sendMessageAndGetCompletable(
                new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_REQUEST));
    }

    /**
     * Bootstraps a layout server.
     * @param l     The layout to bootstrap with.
     * @return      A completable future which will return TRUE if the
     *              bootstrap was successful, false otherwise.
     */
    public CompletableFuture<Boolean>  bootstrapLayout(Layout l)
    {
        return router.sendMessageAndGetCompletable(
                new LayoutMsg(l, CorfuMsg.CorfuMsgType.LAYOUT_BOOTSTRAP));
    }

    /**
     * Begins phase 1 of a Paxos round with a prepare message.
     * @param rank  The rank to use for the prepare.
     * @return      True, if the prepare was successful.
     *              Otherwise, the completablefuture completes exceptionally
     *              with OutrankedException.
     */
    public CompletableFuture<LayoutPrepareResponse> prepare(long rank)
    {
        return router.sendMessageAndGetCompletable(
                new LayoutRankMsg(null, rank, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE)
        );
    }

    /**
     * Begins phase 2 of a Paxos round with a propose message.
     * @param rank      The rank to use for the propose. It should be the same
     *                  rank from a successful prepare (phase 1).
     * @param layout    The layout to install for phase 2.
     * @return          True, if the propose was successful.
     *                  Otherwise, the completablefuture completes exceptionally
     *                  with OutrankedException.
     */
    public CompletableFuture<Boolean> propose(long rank, Layout layout)
    {
        return router.sendMessageAndGetCompletable(
                new LayoutRankMsg(layout, rank, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE)
        );
    }

    /**
     * Informs the server that the rank has been committed to a quorum.
     * @param rank  The rank to use for the prepare.
     * @return      True, if the commit was successful.
     *              Otherwise, the completablefuture completes exceptionally
     *              with OutrankedException.
     */
    public CompletableFuture<Boolean> committed(long rank, Layout layout)
    {
        return router.sendMessageAndGetCompletable(
                new LayoutRankMsg(layout, rank, CorfuMsg.CorfuMsgType.LAYOUT_COMMITTED)
        );
    }

}
