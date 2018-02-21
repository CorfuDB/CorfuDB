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
import lombok.Getter;
import lombok.Setter;

/**
 * A client to the layout server.
 * <p>
 * In addition to being used by clients to obtain the layout and to report errors,
 * The layout client is also used by layout servers to initiate a Paxos-based protocol
 * for determining the next layout.
 * </p>
 * Created by zlokhandwala on 2/20/18.
 */
public class LayoutSenderClient implements IClient {

    @Getter
    @Setter
    private IClientRouter router;

    private final long epoch;

    public LayoutSenderClient(IClientRouter router, long epoch) {
        this.router = router;
        this.epoch = epoch;
    }

    /**
     * Retrieves the layout from the endpoint, asynchronously.
     *
     * @return A future which will be completed with the current layout.
     */
    public CompletableFuture<Layout> getLayout() {
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_REQUEST
                .payloadMsg(epoch).setEpoch(epoch));
    }

    /**
     * Bootstraps a layout server.
     *
     * @param l The layout to bootstrap with.
     * @return A completable future which will return TRUE if the
     * bootstrap was successful, false otherwise.
     */
    public CompletableFuture<Boolean> bootstrapLayout(Layout l) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_BOOTSTRAP
                .payloadMsg(new LayoutBootstrapRequest(l)).setEpoch(epoch));
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
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_PREPARE
                .payloadMsg(new LayoutPrepareRequest(epoch, rank)).setEpoch(epoch));
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
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_PROPOSE
                .payloadMsg(new LayoutProposeRequest(epoch, rank, layout)).setEpoch(epoch));

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
                .payloadMsg(new LayoutCommittedRequest(epoch, layout)).setEpoch(epoch));
    }

    /**
     * Send a force commit layout request to a layout server
     *
     * @param layout the new layout to force commit
     * @return true if it was committed, otherwise false.
     */
    public CompletableFuture<Boolean> force(@Nonnull Layout layout) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_COMMITTED
                .payloadMsg(new LayoutCommittedRequest(true, layout.getEpoch(), layout))
                .setEpoch(epoch));
    }

}
