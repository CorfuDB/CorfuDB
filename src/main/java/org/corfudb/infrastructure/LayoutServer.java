package org.corfudb.infrastructure;

import com.google.common.io.Files;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.protocols.wireprotocol.LayoutRankMsg;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;

/**
 * The layout server serves layouts, which are used by clients to find the
 * Corfu infrastructure.
 *
 * For replication and high availability, the layout server implements a
 * basic Paxos protocol. The layout server functions as a Paxos acceptor,
 * and accepts proposals from clients consisting of a rank and desired
 * layout. The protocol consists of three rounds:
 *
 * 1)   Prepare(rank) - Clients first contact each server with a rank.
 *      If the server responds with ACK, the server promises not to
 *      accept any requests with a rank lower than the given rank.
 *      If the server responds with LAYOUT_PREPARE_REJECT, the server
 *      informs the client of the current high rank and the request is
 *      rejected.
 *
 * 2)   Propose(rank,layout) - Clients then contact each server with
 *      the previously prepared rank and the desired layout. If no other
 *      client has sent a prepare with a higher rank, the layout is
 *      persisted, and the server begins serving that layout to other
 *      clients. If the server responds with LAYOUT_PROPOSE_REJECT,
 *      either another client has sent a prepare with a higher rank,
 *      or this was a propose of a previously accepted rank.
 *
 * 3)   Committed(rank) - Clients then send a hint to each layout
 *      server that a new rank has been accepted by a quorum of
 *      servers.
 *
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class LayoutServer implements IServer {

    /** The options map. */
    Map<String,Object> opts;

    /** The current layout. */
    Layout currentLayout;

    /** The current phase 1 rank */
    long phase1Rank;

    /** The current phase 2 rank, which should be equal to the epoch. */
    long phase2Rank;

    /** Th layout file, or null if in memory. */
    File layoutFile;

    public LayoutServer(Map<String, Object> opts)
    {
        this.opts = opts;

        if (opts.get("--log-path") != null)
        {
            layoutFile = new File(opts.get("--log-path") + File.separator + "layout");
        }

        if ((Boolean)opts.get("--single"))
        {
            String localAddress =  opts.get("--address") + ":" + opts.get("<port>");
            log.info("Single-node mode requested, initializing layout with single log unit and sequencer at {}.",
                    localAddress);
            currentLayout = new Layout(
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
            );

            phase1Rank = phase2Rank = 0;
            saveCurrentLayout();
        }
        else
        {
            try {
                if (layoutFile == null) {
                    log.info("Layout server started, but in-memory mode set without bootstrap. " +
                            "Starting uninitialized layout server.");
                    currentLayout = null;
                }
                else if (!layoutFile.exists())
                {
                    log.warn("Layout server started, but no layout log found. Starting uninitialized layout server.");
                    currentLayout = null;
                }
                else {
                    String l = Files.toString(layoutFile, Charset.defaultCharset());
                    currentLayout = Layout.fromJSONString(l);
                    phase1Rank = phase2Rank = currentLayout.getEpoch();
                    log.info("Layout server started with layout from disk: {}.", currentLayout);
                }
            }
            catch (Exception e)
            {
                log.error("Error reading from layout server", e);
                currentLayout = null;
            }
        }
    }

    /** Save the current layout to disk, if not in-memory mode.
     *
     */
    public void saveCurrentLayout() {
        if (layoutFile != null)
        {
            try {
                Files.write(currentLayout.asJSONString().getBytes(), layoutFile);
                log.info("Layout epoch {} saved to disk.", currentLayout.getEpoch());
            } catch (Exception e)
            {
                log.error("Error saving layout to disk!", e);
            }
        }
    }

    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        // This server has not been bootstrapped yet, ignore ALL requests except for LAYOUT_BOOTSTRAP
        if (currentLayout == null)
        {
            if (msg.getMsgType().equals(CorfuMsg.CorfuMsgType.LAYOUT_BOOTSTRAP))
            {
                log.info("Bootstrap with new layout={}", ((LayoutMsg)msg).getLayout());
                currentLayout = ((LayoutMsg)msg).getLayout();
                saveCurrentLayout();
                //send a response that the bootstrap was successful.
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
            }
            else {
                log.warn("Received message but not bootstrapped! Message={}", msg);
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_NOBOOTSTRAP));
            }
            return;
        }

        switch (msg.getMsgType())
        {
            case LAYOUT_REQUEST:
                r.sendResponse(ctx, msg, new LayoutMsg(currentLayout, CorfuMsg.CorfuMsgType.LAYOUT_RESPONSE));
            break;
            case LAYOUT_BOOTSTRAP:
                // We are already bootstrapped, bootstrap again is not allowed.
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.NACK));
            break;
            case LAYOUT_PREPARE:
            {
                LayoutRankMsg m = (LayoutRankMsg)msg;
                // This is a prepare. If the rank is less than or equal to the phase 1 rank, reject.
                if (m.getRank() <= phase1Rank) {
                    log.debug("Rejected phase 1 prepare of rank={}, phase1Rank={}", m.getRank(), phase1Rank);
                    r.sendResponse(ctx, msg, new LayoutRankMsg(null, phase1Rank, CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_REJECT));
                }
                else
                {
                    phase1Rank = ((LayoutRankMsg) msg).getRank();
                    log.debug("New phase 1 rank={}", phase1Rank);
                    r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
                }
            }
            break;
            case LAYOUT_PROPOSE:
            {
                LayoutRankMsg m = (LayoutRankMsg)msg;
                // This is a propose. If the rank is less than or equal to the phase 1 rank, reject.
                if (m.getRank() != phase1Rank) {
                    log.debug("Rejected phase 2 propose of rank={}, phase1Rank={}", m.getRank(), phase1Rank);
                    r.sendResponse(ctx, msg, new LayoutRankMsg(null, phase1Rank, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT));
                }
                // In addition, if the rank is equal to the current phase 2 rank (already accepted message), reject.
                else if (m.getRank() == phase2Rank)
                {
                    log.debug("Rejected phase 2 propose of rank={}, phase2Rank={}", m.getRank(), phase2Rank);
                    r.sendResponse(ctx, msg, new LayoutRankMsg(null, phase2Rank, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT));
                }
                else
                {
                    log.debug("New phase 2 rank={}, old rank={}, layout={}", ((LayoutRankMsg) msg).getRank(), phase2Rank,
                            ((LayoutRankMsg) msg).getLayout());
                    phase2Rank = ((LayoutRankMsg) msg).getRank();
                    currentLayout =  ((LayoutRankMsg) msg).getLayout();
                    saveCurrentLayout();
                    r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
                }
            }
            break;
            case LAYOUT_COMMITTED:
            {
                // Currently we just acknowledge the commit. We could do more than
                // just that.
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
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
}
