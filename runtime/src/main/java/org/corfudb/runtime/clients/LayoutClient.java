package org.corfudb.runtime.clients;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nonnull;

import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.runtime.view.Layout;

import static org.corfudb.protocols.service.CorfuProtocolLayout.getLayoutRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getBootstrapLayoutRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getPrepareLayoutRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getProposeLayoutRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getCommitLayoutRequestMsg;

/**
 * A client to the layout server.
 * <p>
 * The layout client is used by layout servers to initiate a Paxos-based protocol
 * for determining the next layout.
 * </p>
 * Created by mwei on 12/9/15.
 */
public class LayoutClient extends AbstractClient {

    public LayoutClient(IClientRouter router, long epoch, UUID clusterID) {
        super(router, epoch, clusterID);
    }

    /**
     * Retrieves the layout from the endpoint, asynchronously.
     *
     * @return A future which will be completed with the current layout.
     */
    public CompletableFuture<Layout> getLayout() {
        return sendRequestWithFuture(getLayoutRequestMsg(getEpoch()), ClusterIdCheck.IGNORE, EpochCheck.IGNORE);
    }

    /**
     * Bootstraps a layout server.
     *
     * @param l The layout to bootstrap with.
     * @return A completable future which will return TRUE if the
     * bootstrap was successful, false otherwise.
     */
    public CompletableFuture<Boolean> bootstrapLayout(Layout l) {
        return sendRequestWithFuture(getBootstrapLayoutRequestMsg(l), ClusterIdCheck.IGNORE, EpochCheck.IGNORE);
    }

    /**
     * Begins phase 1 of a Paxos round with a prepare message.
     *
     * @param epoch epoch for which the paxos rounds are being run
     * @param rank  The rank to use for the prepare.
     * @return True, if the prepare was successful.
     * Otherwise, the completableFuture completes exceptionally
     * with OutrankedException.
     */
    public CompletableFuture<LayoutPrepareResponse> prepare(long epoch, long rank) {
        return sendRequestWithFuture(getPrepareLayoutRequestMsg(epoch, rank), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
    }

    /**
     * Begins phase 2 of a Paxos round with a propose message.
     *
     * @param epoch  epoch for which the paxos rounds are being run
     * @param rank   The rank to use for the propose. It should be the same
     *               rank from a successful prepare (phase 1).
     * @param layout The layout to install for phase 2.
     * @return True, if the propose was successful.
     * Otherwise, the completableFuture completes exceptionally
     * with OutrankedException.
     */
    public CompletableFuture<Boolean> propose(long epoch, long rank, Layout layout) {
        return sendRequestWithFuture(getProposeLayoutRequestMsg(epoch, rank, layout), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
    }

    /**
     * Informs the server that the proposal (layout) has been committed to a quorum.
     *
     * @param epoch  epoch affiliated with the layout.
     * @param layout Layout to be committed.
     * @return True, if the commit was successful.
     */
    public CompletableFuture<Boolean> committed(long epoch, Layout layout) {
        return sendRequestWithFuture(getCommitLayoutRequestMsg(false, epoch, layout), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
    }

    /**
     * Send a force commit layout request to a layout server
     *
     * @param layout the new layout to force commit
     * @return true if it was committed, otherwise false.
     */
    public CompletableFuture<Boolean> force(@Nonnull Layout layout) {
        return sendRequestWithFuture(getCommitLayoutRequestMsg(true, layout.getEpoch(), layout), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
    }

}
