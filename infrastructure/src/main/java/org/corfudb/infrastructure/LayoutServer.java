package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerThreadFactory.ExceptionHandler;
import org.corfudb.infrastructure.paxos.PaxosDataStore;
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
import org.corfudb.runtime.view.Layout;

/**
 * The layout server serves layouts, which are used by clients to find the
 * Corfu infrastructure.
 *
 * <p>For replication and high availability, the layout server implements
 * Basic Paxos with NACKs. The layout server functions as a Paxos acceptor,
 * and accepts proposals from clients consisting of a rank and desired
 * value (layout). The protocol consists of three rounds:
 *
 * <p>1)   Prepare(rank) - Clients first contact each server with a rank.
 * If the server responds with ACK, the server promises not to
 * accept any requests with a rank lower than the given rank.
 * ACK consists of a triplet (proposed rank, accepted rank, accepted value).
 * Both accepted rank and the value are optional in case nothing has been accepted.
 * If the server responds with NACK (LAYOUT_PREPARE_REJECT), the server
 * informs the client of the current highest proposed rank and the request is
 * rejected.
 *
 * <p>2)   Propose(rank, layout) - Clients then contact each server with
 * the previously prepared rank and the desired value (layout). If no other
 * client has sent a prepare with a higher rank, the layout is
 * persisted, and the server begins serving that layout to other
 * clients. If the server responds with LAYOUT_PROPOSE_REJECT,
 * either another client has sent a prepare with a higher rank,
 * or this was a propose of a previously accepted rank.
 *
 * <p>3)   Committed(rank, layout) - Clients then send a hint to each acceptor
 * (layout server) that a new proposal has been accepted by a quorum of
 * servers. Adding phase three to Classic Paxos serves an important purpose
 * that may not be immediately apparent, namely that the liveness conditions
 * are no longer necessary for progress. With this variant, Classic Paxos can make
 * progress provided either a majority of acceptors are up or at least one acceptor
 * who has been notified of decision is up. As a result, the proposer may return a
 * decided value after communicating with just one acceptor.
 *
 * <p>Created by mwei on 12/8/15.
 */
//TODO Need a janitor to cleanup old phases data and to fill up holes in layout history.
@Slf4j
public class LayoutServer extends AbstractServer {

    private static final long UNINITIALIZED_RANK = -1L;

    @Getter
    private final ServerContext serverContext;

    /**
     * Handler for this server.
     */
    @Getter
    private final CorfuMsgHandler handler =
            CorfuMsgHandler.generateHandler(MethodHandles.lookup(), this);

    @NonNull
    private final ExecutorService executor;

    @NonNull
    private final PaxosDataStore paxosDataStore;

    @Override
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        return getState() == ServerState.READY;
    }

    @Override
    public ExecutorService getExecutor(CorfuMsgType corfuMsgType) {
        return executor;
    }

    @Override
    public List<ExecutorService> getExecutors() {
        return Collections.singletonList(executor);
    }

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

        executor = Executors.newFixedThreadPool(
                serverContext.getLayoutServerThreadCount(),
                new ServerThreadFactory("layoutServer-", new ExceptionHandler())
        );

        if (serverContext.installSingleNodeLayoutIfAbsent()) {
            setLayoutInHistory(getCurrentLayout());
        }
    }

    private boolean isBootstrapped(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (getCurrentLayout() == null) {
            log.warn("Received message but not bootstrapped! Message={}", msg);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.LAYOUT_NOBOOTSTRAP));
            return false;
        }
        return true;
    }

    // Helper Methods

    /**
     * Handle a layout request message.
     *
     * @param msg              corfu message containing LAYOUT_REQUEST
     * @param ctx              netty ChannelHandlerContext
     * @param r                server router
     */
    @ServerHandler(type = CorfuMsgType.LAYOUT_REQUEST)
    public synchronized void handleMessageLayoutRequest(CorfuPayloadMsg<Long> msg,
                                                    ChannelHandlerContext ctx, IServerRouter r) {
        if (!isBootstrapped(msg, ctx, r)) {
            return;
        }

        final long msgEpoch = msg.getPayload();
        final long serverEpoch = getServerEpoch();

        if (msgEpoch <= serverEpoch) {
            r.sendResponse(ctx, msg, new LayoutMsg(getCurrentLayout(), CorfuMsgType
                    .LAYOUT_RESPONSE));
        } else {
            // else the client is somehow ahead of the server.
            //TODO figure out a strategy to deal with this situation
            r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, serverEpoch));
            log.warn("handleMessageLayoutRequest: Message Epoch {} ahead of Server epoch {}",
                    msgEpoch, serverEpoch);
        }
    }

    /**
     * Sets the new layout if the server has not been bootstrapped with one already.
     *
     * @param msg corfu message containing LAYOUT_BOOTSTRAP
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.LAYOUT_BOOTSTRAP)
    public synchronized void handleMessageLayoutBootstrap(
            @NonNull CorfuPayloadMsg<LayoutBootstrapRequest> msg,
            ChannelHandlerContext ctx,
            @NonNull IServerRouter r) {

        if (getCurrentLayout() == null) {
            log.info("handleMessageLayoutBootstrap: Bootstrap with new layout={}, {}",
                    msg.getPayload().getLayout(), msg);
            setCurrentLayout(msg.getPayload().getLayout());
            serverContext.setServerEpoch(getCurrentLayout().getEpoch(), r);
            //send a response that the bootstrap was successful.
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        } else {
            // We are already bootstrapped, bootstrap again is not allowed.
            log.warn("handleMessageLayoutBootstrap: Got a request to bootstrap a server which is "
                    + "already bootstrapped, rejecting!");
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP));
        }
    }

    private boolean isSameEpoch(
            long payloadEpoch,
            @NonNull CorfuPayloadMsg msg,
            @NonNull ChannelHandlerContext ctx,
            @NonNull IServerRouter router) {
        final long serverEpoch = getServerEpoch();

        // Check if the propose is for the correct epoch
        if (payloadEpoch != serverEpoch) {
            router.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, serverEpoch));
            log.trace("Incoming message with wrong epoch, got {} expected {}, message was: {}",
                    payloadEpoch, serverEpoch, msg);
            return false;
        }

        return true;
    }

    /**
     * Phase I: Prepare
     *
     * Each acceptor stores the last promised rank and last accepted proposal. When an
     * acceptor receives prepare(rank_msg), if rank_msg is the first rank promised or
     * if rank_msg is greater than the last rank promised, then rank_msg is written to storage and
     * the acceptor replies with promise(e_msg, rank_acc, value_acc). (rank_acc, value_acc) is the
     * last accepted proposal (if present).
     *
     * Original Algorithm:
     *
     * prepare(rank_msg) received from proposer
     *   if rank_pro = nil ∨ rank_msg ≥ rank_pro then (1)
     *     rank_pro ← rank_msg
     *     send promise(rank_msg, rank_acc, value_acc) to proposer
     *   else
     *     send no-promise(rank_msg, rank_pro) to proposer
     *
     * The only deviation from the original algorithm is (1) where instead of doing
     * rank_msg ≥ rank_pro, we do rank_msg > rank_pro. This does not have any impact on
     * correctness.
     *
     * Warning: Do not try to change the way the synchronization is done. Paxos is meant to be
     * a message passing algorithm, meaning that only one message can be processed at
     * any point in time.
     *
     * Please take a look at Technical Report UCAM-CL-TR-935 ISSN 1476-2986 for more information.
     *
     * @param msg corfu message containing LAYOUT_PREPARE
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.LAYOUT_PREPARE)
    public synchronized void handleMessageLayoutPrepare(
            @NonNull CorfuPayloadMsg<LayoutPrepareRequest> msg,
            ChannelHandlerContext ctx,
            @NonNull IServerRouter r) {

        // Check if the prepare if we are bootstrapped.
        if (!isBootstrapped(msg, ctx, r)) {
            return;
        }

        final long payloadEpoch = msg.getPayload().getEpoch();
        final Rank payloadRank = new Rank(msg.getPayload().getRank(), msg.getClientID());
        final Rank highestProposedRank = getProposedRank(payloadEpoch);
        final Layout acceptedLayout = getAcceptedLayout(payloadEpoch);
        final Rank highestAcceptedRank = Optional.ofNullable(getAcceptedRank(payloadEpoch))
                .orElse(new Rank(UNINITIALIZED_RANK, msg.getClientID()));

        // Check if the prepare is for the correct epoch.
        if (!isSameEpoch(payloadEpoch, msg, ctx, r)) {
            return;
        }

        // This is a prepare. If the rank is less than or equal to the phase 1 rank, reject.
        if (highestProposedRank == null || payloadRank.greaterThan(highestProposedRank)) {
            setProposeRank(payloadRank, payloadEpoch);
            log.debug("Phase I: New proposedRank = {}", getProposedRank(payloadEpoch));
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PREPARE_ACK.payloadMsg(new
                    LayoutPrepareResponse(highestAcceptedRank.getRank(), acceptedLayout)));
        } else {
            // Return the layout with the highest rank proposed before.
            log.debug("Phase I: Rejected prepare: payloadRank = {}, proposedRank = {}",
                    payloadRank, highestProposedRank);
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PREPARE_REJECT.payloadMsg(new
                    LayoutPrepareResponse(highestProposedRank.getRank(), acceptedLayout)));
        }
    }

    /**
     * Phase II: Propose
     *
     * Each acceptor receives a propose(rank_msg, value_msg). If rank_msg is the first rank
     * promised or if it is equal to the last promised rank, then the accepted proposal is
     * updated and the acceptor replies with accept(rank_msg).
     *
     * Note: This implementation of propose deviates from the original Paxos implementation.
     *
     * Original Algorithm:
     *
     * propose(rank_msg, value_msg) received from proposer
     *   if rank_pro = nil ∨ rank_msg ≥ rank_pro then
     *     rank_pro ← rank_msg
     *     rank_acc ← rank_msg
     *     value acc ← value_msg,
     *     send accept(rank_msg) to proposer
     *   else
     *     send no-accept(rank_msg, rank_pro) to proposer
     *
     * Current Implementation:
     *
     * propose(rank_msg, value_msg) received from proposer
     *
     *   if rank_pro = nil then (1)
     *     send no-accept(rank_msg, rank_pro) to proposer
     *   if rank_pro ≠ rank_msg then (2)
     *     send no-accept(rank_msg, rank_pro) to proposer
     *   else (3)
     *     rank_acc ← rank_msg
     *     value acc ← value_msg,
     *     send accept(rank_msg) to proposer
     *
     * Technically, Step 1 is not required, since we are already doing an equality check in
     * Step 2. It is there mostly for clarification reasons.
     *
     * Rather than checking in Step 2 if the message rank is greater than or equal to highest
     * proposed rank, we are only checking for equality, This has an effect on the quorum
     * intersection between Phase I and II. The implication is that the any acceptor that
     * accepted a value must have participated in the prepare phase (Phase I) for that exact client
     * and rank. More formally:
     * Current Implementation: Q1 \ Q2 = ∅ : |Q1| = |Q1| = n/2 + 1
     * Original Algorithm:     |Q1 ∩ Q2| = n/2 + 1
     *
     * Notice that we are not setting the proposed rank in Step 3. This is not required since we
     * already know that the message rank is equal to the highest proposed rank.
     *
     * Warning: Do not try to change the way the synchronization is done. Paxos is meant to be
     * a message passing algorithm, meaning that only one message can be processed at
     * any point in time.
     *
     * Please take a look at Technical Report UCAM-CL-TR-935 ISSN 1476-2986 for more information.
     *
     * @param msg corfu message containing LAYOUT_PROPOSE
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.LAYOUT_PROPOSE)
    public synchronized void handleMessageLayoutPropose(
            @NonNull CorfuPayloadMsg<LayoutProposeRequest> msg,
            ChannelHandlerContext ctx,
            @NonNull IServerRouter r) {

        if (!isBootstrapped(msg, ctx, r)) {
            return;
        }

        final long payloadEpoch = msg.getPayload().getEpoch();
        final Rank payloadRank = new Rank(msg.getPayload().getRank(), msg.getClientID());
        final Rank highestProposedRank = getProposedRank(payloadEpoch);
        final Layout messageValue = msg.getPayload().getLayout();

        // Check if the prepare is for the correct epoch.
        if (!isSameEpoch(payloadEpoch, msg, ctx, r)) {
            return;
        }

        // This is a propose. If no prepare, reject.
        if (highestProposedRank == null) {
            log.debug("Phase II: Rejected propose of rank = {}, proposedRank = none",
                    highestProposedRank);
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PROPOSE_REJECT.payloadMsg(new
                    LayoutProposeResponse(-1)));
        } else if (!payloadRank.equals(highestProposedRank)) {
            log.debug("Phase II: Rejected propose of rank = {}, proposedRank = {}",
                    payloadRank, highestProposedRank);
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PROPOSE_REJECT.payloadMsg(new
                    LayoutProposeResponse(highestProposedRank.getRank())));
        } else {
            log.debug("Phase II: New acceptedRank = {}, layout = {}",
                    payloadRank, messageValue);
            setProposeRank(payloadRank, payloadEpoch);
            setAcceptedData(new AcceptedData(payloadRank, messageValue), payloadEpoch);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        }
    }

    /**
     * Accepts any committed layouts for the current epoch or newer epochs.
     * As part of the accept, the server changes it's current layout and epoch.
     *
     * @param msg corfu message containing LAYOUT_COMMITTED
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    // TODO If a server does not get SET_EPOCH layout commit message cannot reach it
    // TODO as this message is not set to ignore EPOCH.
    // TODO How do we handle holes in history if we let in layout commit message. Maybe we have a
    // hole filling process
    @ServerHandler(type = CorfuMsgType.LAYOUT_COMMITTED)
    public synchronized void handleMessageLayoutCommit(
            @NonNull CorfuPayloadMsg<LayoutCommittedRequest> msg,
            ChannelHandlerContext ctx,
            @NonNull IServerRouter r) {

        if (msg.getPayload().getForce()) {
            forceLayout(msg, ctx, r);
            return;
        }

        if (!isBootstrapped(msg, ctx, r)) {
            return;
        }

        final long payloadEpoch = msg.getPayload().getEpoch();
        final long serverEpoch = getServerEpoch();
        final Layout msgLayout = msg.getPayload().getLayout();

        if (payloadEpoch < serverEpoch) {
            r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, serverEpoch));
            return;
        }

        setCurrentLayout(msgLayout);
        serverContext.setServerEpoch(payloadEpoch, r);
        log.info("New layout committed: {}", msgLayout);
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
    }

    /**
     * Force layout enables the server to bypass consensus
     * and accept a new layout.
     *
     * @param msg              corfu message containing LAYOUT_FORCE
     * @param ctx              netty ChannelHandlerContext
     * @param r                server router
     */
    private synchronized void forceLayout(@Nonnull CorfuPayloadMsg<LayoutCommittedRequest> msg,
                                               @Nonnull ChannelHandlerContext ctx,
                                               @Nonnull IServerRouter r) {
        final long payloadEpoch = msg.getPayload().getEpoch();
        final long serverEpoch = getServerEpoch();

        if (payloadEpoch != serverEpoch) {
            // return can't force old epochs
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            log.warn("forceLayout: Trying to force a layout with an old epoch. Layout {}, " +
                    "current epoch {}", payloadEpoch, serverEpoch);
            return;
        }

        final Layout msgLayout = msg.getPayload().getLayout();

        setCurrentLayout(msgLayout);
        serverContext.setServerEpoch(msgLayout.getEpoch(), r);
        log.warn("forceLayout: Forcing new layout {}", msgLayout);
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
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


    public Rank getProposedRank(long epoch) {
        return paxosDataStore
                .getPhase1Rank(epoch)
                .orElse(null);
    }


    public void setProposeRank(Rank rank, long epoch) {
        paxosDataStore.setPhase1Rank(rank, epoch);
    }

    public void setAcceptedData(AcceptedData acceptedData, long epoch) {
        paxosDataStore.setAcceptedData(acceptedData, epoch);
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
    public Rank getAcceptedRank(long epoch) {
        return paxosDataStore
                .getPhase2Data(epoch)
                .map(AcceptedData::getRank)
                .orElse(null);
    }

    /**
     * Returns the proposed layout received in phase 2 data.
     *
     * @return the proposed layout
     */

    public Layout getAcceptedLayout(long epoch) {
        return paxosDataStore
                .getPhase2Data(epoch)
                .map(AcceptedData::getLayout)
                .orElse(null);
    }

    /**
     * Return accepted data for the given epoch.
     *
     * @param epoch that we are interested in
     * @return {@link Optional} {@link AcceptedData}
     */
    @VisibleForTesting
    Optional<AcceptedData> getPhase2Data(long epoch) {
        return paxosDataStore.getPhase2Data(epoch);
    }
}
