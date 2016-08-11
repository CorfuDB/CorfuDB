package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.protocols.wireprotocol.LayoutRankMsg;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;

import java.util.Collections;
import java.util.List;
import java.util.Map;

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

    public static final String PREFIX_LAYOUT = "LAYOUT";
    public static final String KEY_LAYOUT = "CURRENT";
    public static final String PREFIX_PHASE_1 = "PHASE_1";
    public static final String KEY_SUFFIX_PHASE_1 = "RANK";
    public static final String PREFIX_PHASE_2 = "PHASE_2";
    public static final String KEY_SUFFIX_PHASE_2 = "DATA";
    public static final String PREFIX_LAYOUTS = "LAYOUTS";

    /**
     * The options map.
     */
    Map<String, Object> opts;

    /**
     * The server router.
     */
    @Getter
    IServerRouter serverRouter;

    private DataStore dataStore;

    public LayoutServer(Map<String, Object> opts, IServerRouter serverRouter) {
        this.opts = opts;
        this.serverRouter = serverRouter;
        this.dataStore = new DataStore(opts);

        if ((Boolean) opts.get("--single")) {
            String localAddress = opts.get("--address") + ":" + opts.get("<port>");
            log.info("Single-node mode requested, initializing layout with single log unit and sequencer at {}.",
                    localAddress);
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
        } else {
            Layout currentLayout = getCurrentLayout();
            if (currentLayout != null) {
                getServerRouter().setServerEpoch(currentLayout.getEpoch());
            }
            log.info("Layout server started with layout from disk: {}.", currentLayout);
        }
    }

    //TODO need to figure out if we need to send the complete Rank object in the responses
    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (isShutdown()) return;
        // This server has not been bootstrapped yet, ignore ALL requests except for LAYOUT_BOOTSTRAP
        if (getCurrentLayout() == null && !msg.getMsgType().equals(CorfuMsg.CorfuMsgType.LAYOUT_BOOTSTRAP)) {
            log.warn("Received message but not bootstrapped! Message={}", msg);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_NOBOOTSTRAP));
            return;
        }
        switch (msg.getMsgType()) {
            case LAYOUT_REQUEST:
                r.sendResponse(ctx, msg, new LayoutMsg(getCurrentLayout(), CorfuMsg.CorfuMsgType.LAYOUT_RESPONSE));
                break;
            case LAYOUT_BOOTSTRAP:
                handleMessageLayoutBootStrap(msg, ctx, r);
                break;
            case LAYOUT_PREPARE:
                handleMessageLayoutPrepare((LayoutRankMsg) msg, ctx, r);
                break;
            case LAYOUT_PROPOSE:
                handleMessageLayoutPropose((LayoutRankMsg) msg, ctx, r);
                break;
            case LAYOUT_COMMITTED: {
                handleMessageLayoutCommit((LayoutRankMsg) msg, ctx, r);
            }
            break;
            default:
                log.warn("Unknown message type {} passed to handler!", msg.getMsgType());
                throw new RuntimeException("Unsupported message passed to handler!");
        }
    }

    @Override
    public void reset() {
    }

    // Helper Methods

    /**
     * Sets the new layout if the server has not been bootstrapped with one already.
     *
     * @param msg
     * @param ctx
     * @param r
     */
    public synchronized void handleMessageLayoutBootStrap(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (getCurrentLayout() == null) {
            log.info("Bootstrap with new layout={}", ((LayoutMsg) msg).getLayout());
            setCurrentLayout(((LayoutMsg) msg).getLayout());
            getServerRouter().setServerEpoch(getCurrentLayout().getEpoch());
            //send a response that the bootstrap was successful.
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
        } else {
            // We are already bootstrapped, bootstrap again is not allowed.
            log.warn("Got a request to bootstrap a server which is already bootstrapped, rejecting!");
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP));
        }
    }

    /**
     * Accepts a prepare message if the rank is higher than any accepted so far.
     * @param msg
     * @param ctx
     * @param r
     */
    // TODO this can work under a separate lock for this step as it does not change the global components
    public synchronized void handleMessageLayoutPrepare(LayoutRankMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        Rank prepareRank = getRank(msg);
        Rank phase1Rank = getPhase1Rank();
        Layout proposedLayout = getProposedLayout();
        // This is a prepare. If the rank is less than or equal to the phase 1 rank, reject.
        if (phase1Rank != null && prepareRank.compareTo(phase1Rank) <= 0) {
            log.debug("Rejected phase 1 prepare of rank={}, phase1Rank={}", prepareRank, phase1Rank);
            r.sendResponse(ctx, msg, new LayoutRankMsg(proposedLayout, phase1Rank.getRank(), CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_REJECT));
        } else {
            setPhase1Rank(prepareRank);
            log.debug("New phase 1 rank={}", getPhase1Rank());
            r.sendResponse(ctx, msg, new LayoutRankMsg(proposedLayout, prepareRank.getRank(), CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_ACK));
        }
    }

    /**
     * Accepts a proposal for which it had accepted in the prepare phase.
     * A minor optimization is to reject any duplicate propose messages.
     * @param msg
     * @param ctx
     * @param r
     */
    public synchronized void handleMessageLayoutPropose(LayoutRankMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        Rank proposeRank = getRank(msg);
        Layout proposeLayout = msg.getLayout();
        Rank phase1Rank = getPhase1Rank();
        Rank phase2Rank = getPhase2Rank();
        // This is a propose. If the rank is less than or equal to the phase 1 rank, reject.
        if ((phase1Rank == null ) || (phase1Rank != null && proposeRank.compareTo(phase1Rank) != 0)) {
            log.debug("Rejected phase 2 propose of rank={}, phase1Rank={}", proposeRank, phase1Rank);
            r.sendResponse(ctx, msg, new LayoutRankMsg(null, phase1Rank.getRank(), CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT));
            return;
        }
        // In addition, if the rank is equal to the current phase 2 rank (already accepted message), reject.
        // This can happen in case of duplicate messages.
        if (phase2Rank != null && proposeRank.compareTo(phase2Rank) == 0) {
            log.debug("Rejected phase 2 propose of rank={}, phase2Rank={}", proposeRank, phase2Rank);
            r.sendResponse(ctx, msg, new LayoutRankMsg(null, phase2Rank.getRank(), CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT));
            return;
        }

        log.debug("New phase 2 rank={},  layout={}", proposeRank, proposeLayout);
        setPhase2Data(new Phase2Data(proposeRank, proposeLayout));
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
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
    public synchronized void handleMessageLayoutCommit(LayoutRankMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if(msg.getEpoch() < serverRouter.getServerEpoch()) {
            return;
        }
        Layout commitLayout = msg.getLayout();
        setCurrentLayout(commitLayout);
        serverRouter.setServerEpoch(commitLayout.getEpoch());
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
    }

    public Layout getCurrentLayout() {
        return dataStore.get(Layout.class, PREFIX_LAYOUT, KEY_LAYOUT);
    }

    public void setCurrentLayout(Layout layout) {
        dataStore.put(Layout.class, PREFIX_LAYOUT, KEY_LAYOUT, layout);
        // set the layout in history as well
        setLayoutInHistory(layout);
    }

    public Rank getPhase1Rank() {
        return dataStore.get(Rank.class, PREFIX_PHASE_1, serverRouter.getServerEpoch() + KEY_SUFFIX_PHASE_1);
    }

    public void setPhase1Rank(Rank rank) {
        dataStore.put(Rank.class, PREFIX_PHASE_1, serverRouter.getServerEpoch() + KEY_SUFFIX_PHASE_1, rank);
    }

    public Phase2Data getPhase2Data() {
        return dataStore.get(Phase2Data.class, PREFIX_PHASE_2, serverRouter.getServerEpoch() + KEY_SUFFIX_PHASE_2);
    }

    public void setPhase2Data(Phase2Data phase2Data) {
        dataStore.put(Phase2Data.class, PREFIX_PHASE_2, serverRouter.getServerEpoch() + KEY_SUFFIX_PHASE_2, phase2Data);
    }

    public void setLayoutInHistory(Layout layout) {
        dataStore.put(Layout.class, PREFIX_LAYOUTS, String.valueOf(layout.getEpoch()), layout);
    }

    public List<Layout> getLayoutHistory() {
        List<Layout> layouts = dataStore.getAll(Layout.class, PREFIX_LAYOUTS);
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

    private Rank getRank(LayoutRankMsg msg) {
        return new Rank(msg.getRank(), msg.getClientID());
    }
}