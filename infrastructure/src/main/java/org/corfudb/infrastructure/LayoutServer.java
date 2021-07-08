
package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;
import io.netty.channel.ChannelHandlerContext;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.paxos.PaxosDataStore;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.Layout.CommitLayoutRequestMsg;
import org.corfudb.runtime.proto.service.Layout.PrepareLayoutRequestMsg;
import org.corfudb.runtime.proto.service.Layout.ProposeLayoutRequestMsg;
import org.corfudb.runtime.view.Layout;

import static org.corfudb.protocols.CorfuProtocolCommon.getLayout;
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getBootstrappedErrorMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getWrongEpochErrorMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getBootstrapLayoutResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getCommitLayoutResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getLayoutResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getPrepareLayoutResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getProposeLayoutResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;

/**
 * The layout server serves layouts, which are used by clients to find the
 * Corfu infrastructure.
 *
 * <p>For replication and high availability, the layout server implements a
 * basic Paxos protocol. The layout server functions as a Paxos acceptor,
 * and accepts proposals from clients consisting of a rank and desired
 * layout. The protocol consists of three rounds:
 *
 * <p>1)   Prepare(rank) - Clients first contact each server with a rank.
 * If the server responds with ACK, the server promises not to
 * accept any requests with a rank lower than the given rank.
 * If the server responds with LAYOUT_PREPARE_REJECT, the server
 * informs the client of the current high rank and the request is
 * rejected.
 *
 * <p>2)   Propose(rank,layout) - Clients then contact each server with
 * the previously prepared rank and the desired layout. If no other
 * client has sent a prepare with a higher rank, the layout is
 * persisted, and the server begins serving that layout to other
 * clients. If the server responds with LAYOUT_PROPOSE_REJECT,
 * either another client has sent a prepare with a higher rank,
 * or this was a propose of a previously accepted rank.
 *
 * <p>3)   Committed(rank, layout) - Clients then send a hint to each layout
 * server that a new rank has been accepted by a quorum of
 * servers.
 *
 * <p>Created by mwei on 12/8/15.
 */
//TODO Finer grained synchronization needed for this class.
//TODO Need a janitor to cleanup old phases data and to fill up holes in layout history.
@Slf4j
@NotThreadSafe
public class LayoutServer extends AbstractServer {

    @Getter
    private final ServerContext serverContext;

    /**
     * RequestHandlerMethods for the Layout server
     */
    @Getter
    private final RequestHandlerMethods handlerMethods =
            RequestHandlerMethods.generateHandler(MethodHandles.lookup(), this);

    @NonNull
    private final ExecutorService executor;

    @NonNull
    private final PaxosDataStore paxosDataStore;

    /**
     * Returns new LayoutServer for context.
     *
     * @param serverContext context object providing settings and objects
     */
    public LayoutServer(@Nonnull ServerContext serverContext) {
        this.serverContext = serverContext;

        this.paxosDataStore = PaxosDataStore.builder()
                .dataStore(serverContext.getDataStore())
                .build();

        // Set the executor to be single-threaded since all the handlers need to be synchronized
        this.executor = serverContext.getExecutorService(1, "layoutServer-");

        if (serverContext.installSingleNodeLayoutIfAbsent()) {
            setLayoutInHistory(getCurrentLayout());
        }
    }

    @Override
    protected void processRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        executor.submit(() -> getHandlerMethods().handle(req, ctx, r));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
    }

    private boolean isBootstrapped(RequestMsg requestMsg) {
        if (getCurrentLayout() == null) {
            log.debug("Received request but not bootstrapped! RequestMsg={}", TextFormat.shortDebugString(requestMsg));
            return false;
        }
        return true;
    }

    // Helper Methods

    /**
     * Handle a layout request message.
     *
     * @param req              corfu message containing LAYOUT_REQUEST
     * @param ctx              netty ChannelHandlerContext
     * @param r                server router
     */
    @VisibleForTesting
    @RequestHandler(type = RequestPayloadMsg.PayloadCase.LAYOUT_REQUEST)
    void handleLayoutRequest(@Nonnull RequestMsg req, @Nonnull ChannelHandlerContext ctx,
                                    @Nonnull IServerRouter r) {
        if (!isBootstrapped(req)) {
            r.sendNoBootstrapError(req.getHeader(), ctx);
            return;
        }

        final long payloadEpoch = req.getPayload().getLayoutRequest().getEpoch();
        final long serverEpoch = getServerEpoch();

        if (payloadEpoch <= serverEpoch) {
            HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            ResponseMsg response = getResponseMsg(responseHeader, getLayoutResponseMsg(getCurrentLayout()));
            r.sendResponse(response, ctx);
        } else {
            log.warn("handleLayoutRequest[{}]: Payload epoch {} ahead of Server epoch {}",
                    req.getHeader().getRequestId(), payloadEpoch, serverEpoch);

            HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            ResponseMsg response = getResponseMsg(responseHeader, getWrongEpochErrorMsg(serverEpoch));
            r.sendResponse(response, ctx);
        }
    }

    /**
     * Sets the new layout if the server has not been bootstrapped with one already.
     *
     * @param req corfu message containing BOOTSTRAP_LAYOUT_REQUEST
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @VisibleForTesting
    @RequestHandler(type = RequestPayloadMsg.PayloadCase.BOOTSTRAP_LAYOUT_REQUEST)
    void handleBootstrapLayoutRequest(@Nonnull RequestMsg req, @Nonnull ChannelHandlerContext ctx,
                                             @Nonnull IServerRouter r) {
        final HeaderMsg requestHeader = req.getHeader();
        HeaderMsg responseHeader;
        ResponseMsg response;

        if (getCurrentLayout() != null) {
            log.warn("handleBootstrapLayoutRequest[{}]: Got a request to bootstrap a server which is "
                    + "already bootstrapped, rejecting!", requestHeader.getRequestId());

            responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            response = getResponseMsg(responseHeader, getBootstrappedErrorMsg());
            r.sendResponse(response, ctx);
            return;
        }

        final Layout layout = getLayout(req.getPayload().getBootstrapLayoutRequest().getLayout());

        if (layout == null) {
            log.warn("handleBootstrapLayoutRequest[{}]: The Layout in the request payload is null",
                    requestHeader.getRequestId());

            responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader,
                    getBootstrapLayoutResponseMsg(false));
        } else if (layout.getClusterId() == null) {
            log.warn("handleBootstrapLayoutRequest[{}]: The layout={} does not have a clusterId",
                    requestHeader.getRequestId(), layout);

            responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader,
                    getBootstrapLayoutResponseMsg(false));
        } else {
            log.info("handleBootstrapLayoutRequest[{}]: Bootstrap with new layout={}", requestHeader.getRequestId(), layout);
            setCurrentLayout(layout);
            serverContext.setServerEpoch(layout.getEpoch(), r);

            responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            response = getResponseMsg(responseHeader,
                    getBootstrapLayoutResponseMsg(true));
        }

        r.sendResponse(response, ctx);
    }

    /**
     * Accepts a prepare message if the rank is higher than any accepted so far.
     *
     * @param req corfu message containing PREPARE_LAYOUT_REQUEST
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    // TODO this can work under a separate lock for this step as it does not change the global components
    @VisibleForTesting
    @RequestHandler(type = RequestPayloadMsg.PayloadCase.PREPARE_LAYOUT_REQUEST)
    void handlePrepareLayoutRequest(@Nonnull RequestMsg req, @Nonnull ChannelHandlerContext ctx,
                                           @Nonnull IServerRouter r) {
        final HeaderMsg requestHeader = req.getHeader();
        final PrepareLayoutRequestMsg payload = req.getPayload().getPrepareLayoutRequest();

        if (!isBootstrapped(req)) {
            r.sendNoBootstrapError(req.getHeader(), ctx);
            return;
        }

        final long payloadEpoch = payload.getEpoch();
        final long serverEpoch = getServerEpoch();

        final Rank phase1Rank = getPhase1Rank(payloadEpoch);
        final Rank prepareRank = new Rank(payload.getRank(), getUUID(requestHeader.getClientId()));

        if (payloadEpoch != serverEpoch) {
            HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            ResponseMsg response = getResponseMsg(responseHeader, getWrongEpochErrorMsg(serverEpoch));
            r.sendResponse(response, ctx);
            return;
        }

        final Layout proposedLayout = getProposedLayout(payloadEpoch);
        HeaderMsg responseHeader;
        ResponseMsg response;

        // If the PREPARE_LAYOUT_REQUEST rank is less than or equal to the highest phase 1 rank, reject.
        if (phase1Rank != null && prepareRank.lessThanEqualTo(phase1Rank)) {
            log.debug("handlePrepareLayoutRequest[{}]: Rejected phase 1 prepare of rank={}, phase1Rank={}",
                    requestHeader.getRequestId(), prepareRank, phase1Rank);

            responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader, getPrepareLayoutResponseMsg(
                    false, phase1Rank.getRank(), proposedLayout));
        } else {
            // Return the layout with the highest rank proposed before.
            Rank highestProposedRank = proposedLayout == null ?
                    new Rank(-1L, getUUID(requestHeader.getClientId())) : getPhase2Rank(payloadEpoch);

            setPhase1Rank(prepareRank, payloadEpoch);
            log.debug("handlePrepareLayoutRequest[{}]: New phase 1 rank={}", requestHeader.getRequestId(), prepareRank);

            responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            response = getResponseMsg(responseHeader, getPrepareLayoutResponseMsg(
                    true, highestProposedRank.getRank(), proposedLayout));
        }

        r.sendResponse(response, ctx);
    }

    /**
     * Accepts a proposal for which it had accepted in the prepare phase.
     * A minor optimization is to reject any duplicate propose messages.
     *
     * @param req corfu message containing PROPOSE_LAYOUT_REQUEST
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @VisibleForTesting
    @RequestHandler(type = RequestPayloadMsg.PayloadCase.PROPOSE_LAYOUT_REQUEST)
    void handleProposeLayoutRequest(@Nonnull RequestMsg req, @Nonnull ChannelHandlerContext ctx,
                                    @Nonnull IServerRouter r) {
        final HeaderMsg requestHeader = req.getHeader();
        final ProposeLayoutRequestMsg payload = req.getPayload().getProposeLayoutRequest();

        if (!isBootstrapped(req)) {
            r.sendNoBootstrapError(req.getHeader(), ctx);
            return;
        }

        final long payloadEpoch = payload.getEpoch();
        final long serverEpoch = getServerEpoch();

        final Rank phase1Rank = getPhase1Rank(payloadEpoch);
        final Rank proposeRank = new Rank(payload.getRank(), getUUID(requestHeader.getClientId()));

        if (payloadEpoch != serverEpoch) {
            HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            ResponseMsg response = getResponseMsg(responseHeader, getWrongEpochErrorMsg(serverEpoch));
            r.sendResponse(response, ctx);
            return;
        }

        HeaderMsg responseHeader;
        ResponseMsg response;

        // If there is not corresponding PREPARE_LAYOUT_REQUEST, reject.
        if (phase1Rank == null) {
            log.debug("handleProposeLayoutRequest[{}]: Rejected phase 2 propose of rank={}, phase1Rank=none",
                    requestHeader.getRequestId(), proposeRank);

            responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader, getProposeLayoutResponseMsg(false, -1L));

            r.sendResponse(response, ctx);
            return;
        }

        // If the rank in PROPOSE_LAYOUT_REQUEST is less than or equal to the highest observed
        // rank from PREPARE_LAYOUT_REQUEST, reject.
        if (!proposeRank.equals(phase1Rank)) {
            log.debug("handleProposeLayoutRequest[{}]: Rejected phase 2 propose of rank={}, phase1Rank={}",
                    requestHeader.getRequestId(), proposeRank, phase1Rank);

            responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader, getProposeLayoutResponseMsg(false, phase1Rank.getRank()));

            r.sendResponse(response, ctx);
            return;
        }

        final Rank phase2Rank = getPhase2Rank(payloadEpoch);
        final Layout proposeLayout = getLayout(payload.getLayout());

        // Make sure that the layout epoch is the same as the PROPOSE_LAYOUT_REQUEST epoch.
        if (proposeLayout == null) {
            log.warn("handleProposeLayoutRequest[{}]: The Layout in the request payload is null",
                    requestHeader.getRequestId());

            responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader, getProposeLayoutResponseMsg(false, phase1Rank.getRank()));

            r.sendResponse(response, ctx);
            return;
        } else if (proposeLayout.getEpoch() != payloadEpoch) {
            log.debug("handleProposeLayoutRequest[{}]: layout {} and payload {} epoch should be the same",
                    requestHeader.getRequestId(), proposeLayout.getEpoch(), payloadEpoch);

            responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader, getProposeLayoutResponseMsg(false, phase1Rank.getRank()));

            r.sendResponse(response, ctx);
            return;
        }

        // In addition, if the rank in PROPOSE_LAYOUT_REQUEST is equal to the current phase 2 rank,
        // reject. This can happen in case of duplicate messages.
        if (proposeRank.equals(phase2Rank)) {
            log.debug("handleProposeLayoutRequest[{}]: Rejected phase 2 propose of rank={}, phase2Rank={}",
                    requestHeader.getRequestId(), proposeRank, phase2Rank);

            responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader, getProposeLayoutResponseMsg(false, phase2Rank.getRank()));

            r.sendResponse(response, ctx);
            return;
        }

        log.debug("handleProposeLayoutRequest[{}]: New phase 2 rank={}, layout={}",
                requestHeader.getRequestId(), proposeRank, proposeLayout);

        setPhase2Data(new Phase2Data(proposeRank, proposeLayout), payloadEpoch);
        responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        response = getResponseMsg(responseHeader, getProposeLayoutResponseMsg(true, proposeRank.getRank()));

        r.sendResponse(response, ctx);
    }

    /**
     * Force layout enables the server to bypass consensus
     * and accept a new layout.
     *
     * @param req            corfu message containing COMMIT_LAYOUT_REQUEST (forced set to TRUE)
     * @param ctx            netty ChannelHandlerContext
     * @param r              server router
     */
    private void forceLayout(@Nonnull RequestMsg req, @Nonnull ChannelHandlerContext ctx,
                             @Nonnull IServerRouter r) {
        final long payloadEpoch = req.getPayload().getCommitLayoutRequest().getEpoch();
        final long serverEpoch = getServerEpoch();
        final HeaderMsg requestHeader = req.getHeader();
        HeaderMsg responseHeader;
        ResponseMsg response;

        if (payloadEpoch != serverEpoch) {
            log.warn("forceLayout[{}]: Trying to force a layout with an old epoch: payloadEpoch={}, serverEpoch={}",
                    requestHeader.getRequestId(), payloadEpoch, serverEpoch);

            responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader, getCommitLayoutResponseMsg(false));
            r.sendResponse(response, ctx);
            return;
        }

        final Layout layout = getLayout(req.getPayload().getCommitLayoutRequest().getLayout());

        setCurrentLayout(layout);
        serverContext.setServerEpoch(layout.getEpoch(), r);
        log.warn("forceLayout[{}]: Forcing new layout={}", requestHeader.getRequestId(), layout);
        responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        response = getResponseMsg(responseHeader, getCommitLayoutResponseMsg(true));
        r.sendResponse(response, ctx);
    }

    /**
     * Accepts any committed layouts for the current epoch or newer epochs.
     * As part of the accept, the server changes it's current layout and epoch.
     *
     * @param req corfu message containing COMMIT_LAYOUT_REQUEST
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    // TODO If a server does not get SEAL layout commit message cannot reach it
    // TODO as this message is not set to ignore EPOCH.
    // TODO How do we handle holes in history if we let in layout commit message. Maybe we have a
    // TODO hole filling process
    @VisibleForTesting
    @RequestHandler(type = RequestPayloadMsg.PayloadCase.COMMIT_LAYOUT_REQUEST)
    void handleCommitLayoutRequest(@Nonnull RequestMsg req, @Nonnull ChannelHandlerContext ctx,
                                          @Nonnull IServerRouter r) {
        final CommitLayoutRequestMsg payload = req.getPayload().getCommitLayoutRequest();

        if (payload.getForced()) {
            forceLayout(req, ctx, r);
            return;
        }

        if (!isBootstrapped(req)) {
            r.sendNoBootstrapError(req.getHeader(), ctx);
            return;
        }

        final long payloadEpoch = payload.getEpoch();
        final long serverEpoch = getServerEpoch();
        final Layout layout = getLayout(payload.getLayout());

        if (payloadEpoch < serverEpoch) {
            HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            ResponseMsg response = getResponseMsg(responseHeader, getWrongEpochErrorMsg(serverEpoch));
            r.sendResponse(response, ctx);
            return;
        }

        setCurrentLayout(layout);
        serverContext.setServerEpoch(payloadEpoch, r);

        HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        ResponseMsg response = getResponseMsg(responseHeader,
                getCommitLayoutResponseMsg(true));

        log.info("handleCommitLayoutRequest[{}]: New layout committed: {}", req.getHeader().getRequestId(), layout);
        r.sendResponse(response, ctx);
    }

    public Layout getCurrentLayout() {
        Layout layout = serverContext.getCurrentLayout();
        if (layout != null) {
            return new Layout(layout);
        } else {
            return null;
        }
    }

    /**
     * Sets the current layout in context DataStore.
     *
     * @param layout layout to set
     */
    public void setCurrentLayout(Layout layout) {
        serverContext.setCurrentLayout(layout);
        // set the layout in history as well
        setLayoutInHistory(layout);
    }

    public Rank getPhase1Rank(long epoch) {
        return paxosDataStore
                .getPhase1Rank(epoch)
                .orElse(null);
    }

    public void setPhase1Rank(Rank rank, long epoch) {
        paxosDataStore.setPhase1Rank(rank, epoch);
    }

    public void setPhase2Data(Phase2Data phase2Data, long epoch) {
        paxosDataStore.setPhase2Data(phase2Data, epoch);
    }

    public void setLayoutInHistory(Layout layout) {
        serverContext.setLayoutInHistory(layout);
    }

    private long getServerEpoch() {
        return serverContext.getServerEpoch();
    }

    /**
     * Returns the phase 2 rank.
     *
     * @return the phase 2 rank
     */
    public Rank getPhase2Rank(long epoch) {
        return paxosDataStore
                .getPhase2Data(epoch)
                .map(Phase2Data::getRank)
                .orElse(null);
    }

    /**
     * Returns the proposed layout received in phase 2 data.
     *
     * @return the proposed layout
     */
    public Layout getProposedLayout(long epoch) {
        return paxosDataStore
                .getPhase2Data(epoch)
                .map(Phase2Data::getLayout)
                .orElse(null);
    }

    /**
     * Return accepted data for the given epoch.
     *
     * @param epoch that we are interested in
     * @return {@link Optional} {@link Phase2Data}
     */
    @VisibleForTesting
    Optional<Phase2Data> getPhase2Data(long epoch) {
        return paxosDataStore.getPhase2Data(epoch);
    }
}
