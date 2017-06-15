package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.LayoutBootstrapRequest;
import org.corfudb.protocols.wireprotocol.LayoutCommittedRequest;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.protocols.wireprotocol.LayoutPrepareRequest;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.protocols.wireprotocol.LayoutProposeRequest;
import org.corfudb.protocols.wireprotocol.LayoutProposeResponse;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;

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

    @Setter
    @Getter
    IClientRouter router;

    /**
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientMsgHandler msgHandler = new ClientMsgHandler(this)
            .generateHandlers(MethodHandles.lookup(), this);


    @ClientHandler(type = CorfuMsgType.LAYOUT_RESPONSE)
    private static Object handleLayoutResponse(CorfuMsg msg,
                                               ChannelHandlerContext ctx, IClientRouter r) {
        return ((LayoutMsg) msg).getLayout();
    }

    @ClientHandler(type = CorfuMsgType.LAYOUT_PREPARE_ACK)
    private static Object handleLayoutPrepareAck(CorfuPayloadMsg<LayoutPrepareRequest> msg,
                                                 ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    @ClientHandler(type = CorfuMsgType.LAYOUT_NOBOOTSTRAP)
    private static Object handleNoBootstrap(CorfuMsg msg,
                                            ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        throw new NoBootstrapException();
    }

    @ClientHandler(type = CorfuMsgType.LAYOUT_PREPARE_REJECT)
    private static Object handlePrepareReject(CorfuPayloadMsg<LayoutPrepareResponse> msg,
                                              ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        LayoutPrepareResponse response = msg.getPayload();
        throw new OutrankedException(response.getRank(), response.getLayout());
    }

    @ClientHandler(type = CorfuMsgType.LAYOUT_PROPOSE_REJECT)
    private static Object handleProposeReject(CorfuPayloadMsg<LayoutProposeResponse> msg,
                                              ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        LayoutProposeResponse response = msg.getPayload();
        throw new OutrankedException(response.getRank());
    }

    @ClientHandler(type = CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP)
    private static Object handleAlreadyBootstrap(CorfuMsg msg,
                                                 ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        throw new AlreadyBootstrappedException();
    }

    /**
     * Retrieves the layout from the endpoint, asynchronously.
     *
     * @return A future which will be completed with the current layout.
     */
    public CompletableFuture<Layout> getLayout() {
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_REQUEST
                .payloadMsg(router.getEpoch()));
    }

    /**
     * Bootstraps a layout server.
     *
     * @param l The layout to bootstrap with.
     * @return A completable future which will return TRUE if the
     *     bootstrap was successful, false otherwise.
     */
    public CompletableFuture<Boolean> bootstrapLayout(Layout l) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_BOOTSTRAP
                .payloadMsg(new LayoutBootstrapRequest(l)));
    }

    /**
     * Begins phase 1 of a Paxos round with a prepare message.
     *
     * @param epoch epoch for which the paxos rounds are being run
     * @param rank  The rank to use for the prepare.
     * @return True, if the prepare was successful.
     *     Otherwise, the completablefuture completes exceptionally
     *     with OutrankedException.
     */
    public CompletableFuture<LayoutPrepareResponse> prepare(long epoch, long rank) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_PREPARE
                .payloadMsg(new LayoutPrepareRequest(epoch, rank)));
    }

    /**
     * Begins phase 2 of a Paxos round with a propose message.
     *
     * @param epoch  epoch for which the paxos rounds are being run
     * @param rank   The rank to use for the propose. It should be the same
     *               rank from a successful prepare (phase 1).
     * @param layout The layout to install for phase 2.
     * @return True, if the propose was successful.
     *     Otherwise, the completablefuture completes exceptionally
     *     with OutrankedException.
     */
    public CompletableFuture<Boolean> propose(long epoch, long rank, Layout layout) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_PROPOSE
                .payloadMsg(new LayoutProposeRequest(epoch, rank, layout)));

    }

    /**
     * Informs the server that the proposal (layout) has been committed to a quorum.
     *
     * @param epoch  epoch affiliated with the layout.
     * @param layout Layout to be committed.
     * @return True, if the commit was successful.
     */
    public CompletableFuture<Boolean> committed(long epoch, Layout layout) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_COMMITTED
                .payloadMsg(new LayoutCommittedRequest(epoch, layout)));
    }

}
