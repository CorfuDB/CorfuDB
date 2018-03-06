package org.corfudb.runtime.clients;

import java.util.concurrent.CompletableFuture;

import javax.annotation.Nonnull;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.LayoutBootstrapRequest;
import org.corfudb.protocols.wireprotocol.LayoutCommittedRequest;
import org.corfudb.protocols.wireprotocol.LayoutPrepareRequest;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.protocols.wireprotocol.LayoutProposeRequest;
import org.corfudb.runtime.view.Layout;

/**
 * A client to the layout server.
 * <p>
 * The layout client is used by layout servers to initiate a Paxos-based protocol
 * for determining the next layout.
 * </p>
 * Created by mwei on 12/9/15.
 */
public class LayoutClient extends AbstractClient {

    public LayoutClient(IClientRouter router, long epoch) {
        super(router, epoch);
    }

    /**
     * Retrieves the layout from the endpoint, asynchronously.
     *
     * @return A future which will be completed with the current layout.
     */
    public CompletableFuture<Layout> getLayout() {
        return sendMessageWithFuture(CorfuMsgType.LAYOUT_REQUEST.payloadMsg(getEpoch()));
    }

    /**
     * Bootstraps a layout server.
     *
     * @param l The layout to bootstrap with.
     * @return A completable future which will return TRUE if the
     * bootstrap was successful, false otherwise.
     */
    public CompletableFuture<Boolean> bootstrapLayout(Layout l) {
        return sendMessageWithFuture(CorfuMsgType.LAYOUT_BOOTSTRAP
                .payloadMsg(new LayoutBootstrapRequest(l)));
    }

    /**
     * Begins phase 1 of a Paxos round with a prepare message.
     *
     * @param epoch epoch for which the paxos rounds are being run
     * @param rank  The rank to use for the prepare.
     * @return True, if the prepare was successful.
     * Otherwise, the completablefuture completes exceptionally
     * with OutrankedException.
     */
    public CompletableFuture<LayoutPrepareResponse> prepare(long epoch, long rank) {
        return sendMessageWithFuture(CorfuMsgType.LAYOUT_PREPARE
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
     * Otherwise, the completablefuture completes exceptionally
     * with OutrankedException.
     */
    public CompletableFuture<Boolean> propose(long epoch, long rank, Layout layout) {
        return sendMessageWithFuture(CorfuMsgType.LAYOUT_PROPOSE
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
        return sendMessageWithFuture(CorfuMsgType.LAYOUT_COMMITTED
                .payloadMsg(new LayoutCommittedRequest(epoch, layout)));
    }

    /**
     * Send a force commit layout request to a layout server
     * @param layout the new layout to force commit
     * @return true if it was committed, otherwise false.
     */
    public CompletableFuture<Boolean> force(@Nonnull Layout layout) {
        return sendMessageWithFuture(CorfuMsgType.LAYOUT_COMMITTED
                .payloadMsg(new LayoutCommittedRequest(true, layout.getEpoch(), layout)));
    }

}
