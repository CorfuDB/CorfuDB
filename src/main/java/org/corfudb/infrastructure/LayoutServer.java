package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.protocols.wireprotocol.LayoutRankMsg;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;

import java.util.Collections;
import java.util.Map;

/**
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

    public LayoutServer(Map<String, Object> opts)
    {
        this.opts = opts;

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
                            Collections.singletonList(localAddress)
                    )),
                    0L
            );

            phase1Rank = phase2Rank = 0;
        }
        else
        {
            log.warn("Layout server started, but no layout log found. Starting uninitialized layout server.");
            currentLayout = null;
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
                phase1Rank = phase2Rank = currentLayout.getEpoch();
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
                else
                {
                    log.debug("New phase 2 rank={}, old rank={}, layout={}", ((LayoutRankMsg) msg).getRank(), phase2Rank,
                            ((LayoutRankMsg) msg).getLayout());
                    phase2Rank = ((LayoutRankMsg) msg).getRank();
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
