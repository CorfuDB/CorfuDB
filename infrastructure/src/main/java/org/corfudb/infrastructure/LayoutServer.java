package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;

import java.lang.invoke.MethodHandles;
import java.util.*;

/**
 * The layout server serves layouts, which are used by clients to find the
 * Corfu infrastructure.
 * <p>
 * For replication and high availability, the layout server implements a
 * basic Paxos protocol. The layout server functions as a Paxos acceptor,
 * and accepts proposals from clients consisting of a rank and desired
 * layout. The protocol consists of three rounds:
 * <p>
 * 1)   Prepare(rank) - Clients first contact each server with a rank.
 * If the server responds with ACK, the server promises not to
 * accept any requests with a rank lower than the given rank.
 * If the server responds with LAYOUT_PREPARE_REJECT, the server
 * informs the client of the current high rank and the request is
 * rejected.
 * <p>
 * 2)   Propose(rank,layout) - Clients then contact each server with
 * the previously prepared rank and the desired layout. If no other
 * client has sent a prepare with a higher rank, the layout is
 * persisted, and the server begins serving that layout to other
 * clients. If the server responds with LAYOUT_PROPOSE_REJECT,
 * either another client has sent a prepare with a higher rank,
 * or this was a propose of a previously accepted rank.
 * <p>
 * 3)   Committed(rank, layout) - Clients then send a hint to each layout
 * server that a new rank has been accepted by a quorum of
 * servers.
 * <p>
 * Created by mwei on 12/8/15.
 */
//TODO Finer grained synchronization needed for this class.
//TODO Need a janitor to cleanup old phases data and to fill up holes in layout history.
@Slf4j
public class LayoutServer extends AbstractServer {

    private static final String PREFIX_LAYOUT = "LAYOUT";
    private static final String KEY_LAYOUT = "CURRENT";
    private static final String PREFIX_PHASE_1 = "PHASE_1";
    private static final String KEY_SUFFIX_PHASE_1 = "RANK";
    private static final String PREFIX_PHASE_2 = "PHASE_2";
    private static final String KEY_SUFFIX_PHASE_2 = "DATA";
    private static final String PREFIX_LAYOUTS = "LAYOUTS";

    /**
     * The options map.
     */
    private final Map<String, Object> opts;

    @Getter
    private final ServerContext serverContext;

    /** Handler for this server */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .generateHandlers(MethodHandles.lookup(), this);

    private static final String metricsPrefix = "corfu.server.layout.";

    public LayoutServer(ServerContext serverContext) {
        this.opts = serverContext.getServerConfig();
        this.serverContext = serverContext;

        if ((Boolean) opts.get("--single"))
            getSingleNodeLayout();
    }

    private void getSingleNodeLayout() {
        String localAddress = opts.get("--address") + ":" + opts.get("<port>");
        setCurrentLayout(new Layout(
                Collections.singletonList(localAddress),
                Collections.singletonList(localAddress),
                Collections.singletonList(new LayoutSegment(
                        Layout.ReplicationMode.CHAIN_REPLICATION,
                        0L,
                        -1L,
                        Collections.singletonList(
                                new Layout.LayoutStripe(
                                        Collections.singletonList(localAddress)
                                )
                        )
                )),
                0L
        ));
    }

    boolean checkBootstrap(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (getCurrentLayout() == null) {
            log.warn("Received message but not bootstrapped! Message={}", msg);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.LAYOUT_NOBOOTSTRAP));
            return false;
        }
        return true;
    }

    // Helper Methods
    @ServerHandler(type=CorfuMsgType.LAYOUT_REQUEST, opTimer=metricsPrefix + "request")
    public synchronized void handleMessageLayoutRequest(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx, IServerRouter r,
                                                        boolean isMetricsEnabled) {
        if (!checkBootstrap(msg, ctx, r)) { return; }
        long epoch = msg.getPayload();
        if (epoch <= serverContext.getServerEpoch()) {
            r.sendResponse(ctx, msg, new LayoutMsg(getCurrentLayout(), CorfuMsgType.LAYOUT_RESPONSE));
            return;
        } else {
            // else the client is somehow ahead of the server.
            //TODO figure out a strategy to deal with this situation
            long serverEpoch = serverContext.getServerEpoch();
            r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, serverEpoch));
            log.warn("Message Epoch {} ahead of Server epoch {}", epoch, serverContext.getServerConfig());
        }
    }

    /**
     * Sets the new layout if the server has not been bootstrapped with one already.
     *
     * @param msg
     * @param ctx
     * @param r
     */
    @ServerHandler(type=CorfuMsgType.LAYOUT_BOOTSTRAP, opTimer=metricsPrefix + "bootstrap")
    public synchronized void handleMessageLayoutBootstrap(CorfuPayloadMsg<LayoutBootstrapRequest> msg, ChannelHandlerContext ctx, IServerRouter r,
                                                          boolean isMetricsEnabled) {
        if (getCurrentLayout() == null) {
            log.info("Bootstrap with new layout={}, {}",  msg.getPayload().getLayout(), msg);
            setCurrentLayout(msg.getPayload().getLayout());
            serverContext.setServerEpoch(getCurrentLayout().getEpoch());
            //send a response that the bootstrap was successful.
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        } else {
            // We are already bootstrapped, bootstrap again is not allowed.
            log.warn("Got a request to bootstrap a server which is already bootstrapped, rejecting!");
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP));
        }
    }

    /** Respond to a epoch change message.
     *
     * @param msg      The incoming message
     * @param ctx       The channel context
     * @param r         The server router.
     */
    @ServerHandler(type=CorfuMsgType.SET_EPOCH, opTimer=metricsPrefix + "set-epoch")
    public synchronized void handleMessageSetEpoch(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx, IServerRouter r,
                                                   boolean isMetricsEnabled) {
        if (!checkBootstrap(msg, ctx, r)) { return; }
        long serverEpoch = getServerEpoch();
        if (msg.getPayload() >= serverEpoch) {
            log.info("Received SET_EPOCH, moving to new epoch {}", msg.getPayload());
            setServerEpoch(msg.getPayload());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        } else {
            log.debug("Rejected SET_EPOCH currrent={}, requested={}", serverEpoch, msg.getPayload());
            r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, serverEpoch));
        }
    }

    /**
     * Accepts a prepare message if the rank is higher than any accepted so far.
     * @param msg
     * @param ctx
     * @param r
     */
    // TODO this can work under a separate lock for this step as it does not change the global components
    @ServerHandler(type=CorfuMsgType.LAYOUT_PREPARE, opTimer=metricsPrefix + "prepare")
    public synchronized void handleMessageLayoutPrepare(CorfuPayloadMsg<LayoutPrepareRequest> msg, ChannelHandlerContext ctx, IServerRouter r,
                                                        boolean isMetricsEnabled) {
        // Check if the prepare is for the correct epoch
        if (!checkBootstrap(msg, ctx, r)) { return; }
        Rank prepareRank = new Rank(msg.getPayload().getRank(), msg.getClientID());
        Rank phase1Rank = getPhase1Rank();
        Layout proposedLayout = getProposedLayout();

        long serverEpoch = getServerEpoch();
        if (msg.getPayload().getEpoch() != serverEpoch) {
            r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, serverEpoch));
            log.trace("Incoming message with wrong epoch, got {}, expected {}, message was: {}", msg.getPayload().getEpoch(), serverEpoch, msg);
            return;
        }


        // This is a prepare. If the rank is less than or equal to the phase 1 rank, reject.
        if (phase1Rank != null && prepareRank.lessThanEqualTo(phase1Rank)) {
            log.debug("Rejected phase 1 prepare of rank={}, phase1Rank={}", prepareRank, phase1Rank);
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PREPARE_REJECT.payloadMsg(new LayoutPrepareResponse(phase1Rank.getRank(), proposedLayout)));
        } else {
            setPhase1Rank(prepareRank);
            log.debug("New phase 1 rank={}", getPhase1Rank());
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PREPARE_ACK.payloadMsg(new LayoutPrepareResponse(prepareRank.getRank(), proposedLayout)));
        }
    }

    /**
     * Accepts a proposal for which it had accepted in the prepare phase.
     * A minor optimization is to reject any duplicate propose messages.
     * @param msg
     * @param ctx
     * @param r
     */
    @ServerHandler(type=CorfuMsgType.LAYOUT_PROPOSE, opTimer=metricsPrefix + "propose")
    public synchronized void handleMessageLayoutPropose(CorfuPayloadMsg<LayoutProposeRequest> msg, ChannelHandlerContext ctx, IServerRouter r,
                                                        boolean isMetricsEnabled) {
        // Check if the propose is for the correct epoch
        if (!checkBootstrap(msg, ctx, r)) { return; }
        Rank proposeRank = new Rank(msg.getPayload().getRank(), msg.getClientID());
        Layout proposeLayout = msg.getPayload().getLayout();
        Rank phase1Rank = getPhase1Rank();
        Rank phase2Rank = getPhase2Rank();

        long serverEpoch = getServerEpoch();

        if (msg.getPayload().getEpoch() != serverEpoch) {
            r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, serverEpoch));
            log.trace("Incoming message with wrong epoch, got {}, expected {}, message was: {}", msg.getPayload().getEpoch(), serverEpoch, msg);
            return;
        }
        // This is a propose. If no prepare, reject.
        if (phase1Rank == null) {
            log.debug("Rejected phase 2 propose of rank={}, phase1Rank=none", proposeRank);
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PROPOSE_REJECT.payloadMsg(new LayoutProposeResponse(-1)));
            return;
        }
        // This is a propose. If the rank is less than or equal to the phase 1 rank, reject.
        if (!proposeRank.equals(phase1Rank)) {
            log.debug("Rejected phase 2 propose of rank={}, phase1Rank={}", proposeRank, phase1Rank);
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PROPOSE_REJECT.payloadMsg(new LayoutProposeResponse(phase1Rank.getRank())));
            return;
        }
        // In addition, if the rank is equal to the current phase 2 rank (already accepted message), reject.
        // This can happen in case of duplicate messages.
        if (phase2Rank != null && proposeRank.equals(phase2Rank)) {
            log.debug("Rejected phase 2 propose of rank={}, phase2Rank={}", proposeRank, phase2Rank);
            r.sendResponse(ctx, msg, CorfuMsgType.LAYOUT_PROPOSE_REJECT.payloadMsg(new LayoutProposeResponse(phase2Rank.getRank())));
            return;
        }

        log.debug("New phase 2 rank={},  layout={}", proposeRank, proposeLayout);
        setPhase2Data(new Phase2Data(proposeRank, proposeLayout));
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
    }

    /**
     * Accepts any committed layouts for the current epoch or newer epochs.
     * As part of the accept, the server changes it's current layout and epoch.
     * @param msg
     * @param ctx
     * @param r
     */
    // TODO If a server does not get SET_EPOCH layout commit message cannot reach it
    // TODO as this message is not set to ignore EPOCH.
    // TODO How do we handle holes in history if let in layout commit message. Maybe we have a hole filling process
    // TODO how do reject the older epoch commits, should it be an explicit NACK.
    @ServerHandler(type=CorfuMsgType.LAYOUT_COMMITTED, opTimer=metricsPrefix + "committed")
    public synchronized void handleMessageLayoutCommit(CorfuPayloadMsg<LayoutCommittedRequest> msg, ChannelHandlerContext ctx, IServerRouter r,
                                                       boolean isMetricsEnabled) {
        Layout commitLayout = msg.getPayload().getLayout();
        if (!checkBootstrap(msg, ctx, r)) { return; }
        long serverEpoch = getServerEpoch();
        if(msg.getPayload().getEpoch() < serverEpoch) {
            r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, serverEpoch));
            return;
        }

        setCurrentLayout(commitLayout);
        setServerEpoch(msg.getPayload().getEpoch());
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
    }

    /**
     * Validate the epoch of a CorfuMsg, and send a WRONG_EPOCH response if
     * the server is in the wrong epoch. Ignored if the message type is reset (which
     * is valid in any epoch).
     *
     * @param msg The incoming message to validate.
     * @param ctx The context of the channel handler.
     * @return True, if the epoch is correct, but false otherwise.
     */
    public boolean validateEpoch(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        long serverEpoch = getServerEpoch();
        if (msg.getEpoch() != serverEpoch) {
            r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, serverEpoch));
            log.trace("Incoming message with wrong epoch, got {}, expected {}, message was: {}", msg.getEpoch(), serverEpoch, msg);
            return false;
        }
        return true;
    }


    public Layout getCurrentLayout() {
        return serverContext.getDataStore().get(Layout.class, PREFIX_LAYOUT, KEY_LAYOUT);
    }

    public void setCurrentLayout(Layout layout) {
        serverContext.getDataStore().put(Layout.class, PREFIX_LAYOUT, KEY_LAYOUT, layout);
        // set the layout in history as well
        setLayoutInHistory(layout);
    }

    public Rank getPhase1Rank() {
        return serverContext.getDataStore().get(Rank.class, PREFIX_PHASE_1, serverContext.getServerEpoch() + KEY_SUFFIX_PHASE_1);
    }

    public void setPhase1Rank(Rank rank) {
        serverContext.getDataStore().put(Rank.class, PREFIX_PHASE_1, serverContext.getServerEpoch() + KEY_SUFFIX_PHASE_1, rank);
    }

    public Phase2Data getPhase2Data() {
        return serverContext.getDataStore().get(Phase2Data.class, PREFIX_PHASE_2, serverContext.getServerEpoch() + KEY_SUFFIX_PHASE_2);
    }

    public void setPhase2Data(Phase2Data phase2Data) {
        serverContext.getDataStore().put(Phase2Data.class, PREFIX_PHASE_2, serverContext.getServerEpoch() + KEY_SUFFIX_PHASE_2, phase2Data);
    }

    public void setLayoutInHistory(Layout layout) {
        serverContext.getDataStore().put(Layout.class, PREFIX_LAYOUTS, String.valueOf(layout.getEpoch()), layout);
    }

    private void setServerEpoch(long serverEpoch) {
        serverContext.setServerEpoch(serverEpoch);
    }

    private long getServerEpoch() {
        return serverContext.getServerEpoch();
    }

    public List<Layout> getLayoutHistory() {
        List<Layout> layouts = serverContext.getDataStore().getAll(Layout.class, PREFIX_LAYOUTS);
        Collections.sort(layouts, (a, b) -> {
            if (a.getEpoch() > b.getEpoch()) {
                return 1;
            } else if (a.getEpoch() < b.getEpoch()) {
                return -1;
            } else {
                return 0;
            }
        });
        return layouts;
    }

    public Rank getPhase2Rank() {
        Phase2Data phase2Data = getPhase2Data();
        if (phase2Data != null) {
            return phase2Data.getRank();
        }
        return null;
    }

    public Layout getProposedLayout() {
        Phase2Data phase2Data = getPhase2Data();
        if (phase2Data != null) {
            return phase2Data.getLayout();
        }
        return null;
    }

    protected void finalize() {
        //
    }
}
