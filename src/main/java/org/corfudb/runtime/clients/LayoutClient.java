package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.*;
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
    public final Set<CorfuMsgType> HandledTypes =
            new ImmutableSet.Builder<CorfuMsgType>()
                    .add(CorfuMsgType.LAYOUT_REQUEST)
                    .add(CorfuMsgType.LAYOUT_RESPONSE)
                    .add(CorfuMsgType.LAYOUT_PREPARE)
                    .add(CorfuMsgType.LAYOUT_BOOTSTRAP)
                    .add(CorfuMsgType.LAYOUT_NOBOOTSTRAP)
                    .add(CorfuMsgType.LAYOUT_PREPARE_ACK)
                    .add(CorfuMsgType.LAYOUT_PREPARE_REJECT)
                    .add(CorfuMsgType.LAYOUT_PROPOSE_REJECT)
                    .add(CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP)
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
                router.completeExceptionally(msg.getRequestID(), new NoBootstrapException());
                break;
            case LAYOUT_PREPARE_ACK: {
                router.completeRequest(msg.getRequestID(), ((CorfuPayloadMsg<LayoutPrepareResponse>) msg).getPayload());
            }
                break;
            case LAYOUT_PREPARE_REJECT: {
                LayoutPrepareResponse response = ((CorfuPayloadMsg<LayoutPrepareResponse>) msg).getPayload();
                router.completeExceptionally(msg.getRequestID(), new OutrankedException(response.getRank(), response.getLayout()));
            }
                break;
            case LAYOUT_PROPOSE_REJECT: {
                LayoutProposeResponse response = ((CorfuPayloadMsg<LayoutProposeResponse>) msg).getPayload();
                router.completeExceptionally(msg.getRequestID(), new OutrankedException(response.getRank()));
            }
                break;
            case LAYOUT_ALREADY_BOOTSTRAP:
                router.completeExceptionally(msg.getRequestID(), new AlreadyBootstrappedException());
                break;
        }
    }



    /**
     * Retrieves the layout from the endpoint, asynchronously.
     * @return A future which will be completed with the current layout.
     */
    public CompletableFuture<Layout> getLayout() {
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_REQUEST.payloadMsg(router.getEpoch()));
    }

    /**
     * Bootstraps a layout server.
     * @param l     The layout to bootstrap with.
     * @return      A completable future which will return TRUE if the
     *              bootstrap was successful, false otherwise.
     */
    public CompletableFuture<Boolean>  bootstrapLayout(Layout l)
    {
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(l)));
    }

    /**
     * Begins phase 1 of a Paxos round with a prepare message.
     * @param epoch epoch for which the paxos rounds are being run
     * @param rank  The rank to use for the prepare.
     * @return      True, if the prepare was successful.
     *              Otherwise, the completablefuture completes exceptionally
     *              with OutrankedException.
     */
    public CompletableFuture<LayoutPrepareResponse> prepare(long epoch, long rank)
    {
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_PREPARE.payloadMsg(new LayoutPrepareRequest(epoch, rank)));
    }

    /**
     * Begins phase 2 of a Paxos round with a propose message.
     * @param epoch     epoch for which the paxos rounds are being run
     * @param rank      The rank to use for the propose. It should be the same
     *                  rank from a successful prepare (phase 1).
     * @param layout    The layout to install for phase 2.
     * @return          True, if the propose was successful.
     *                  Otherwise, the completablefuture completes exceptionally
     *                  with OutrankedException.
     */
    public CompletableFuture<Boolean> propose(long epoch, long rank, Layout layout)
    {
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_PROPOSE.payloadMsg(new LayoutProposeRequest(epoch, rank, layout)));

    }

    /**
     * Informs the server that the proposal (layout) has been committed to a quorum.
     * @param epoch epoch affiliated with the layout.
     * @param layout
     * @return True, if the commit was successful.
     */
    public CompletableFuture<Boolean> committed(long epoch, Layout layout)
    {
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_COMMITTED.payloadMsg(new LayoutCommittedRequest(epoch, layout)));
    }

}
