package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableList;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;
import org.corfudb.infrastructure.wireprotocol.NettyLayoutResponseMsg;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerTokenRequestMsg;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerTokenResponseMsg;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class LayoutServer implements INettyServer {

    /** The options map. */
    Map<String,Object> opts;

    /** The current layout. */
    Layout currentLayout;

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
        }
        else
        {
            log.warn("Layout server started, but no layout log found. Starting uninitialized layout server.");
            currentLayout = null;
        }
    }

    @Override
    public void handleMessage(NettyCorfuMsg msg, ChannelHandlerContext ctx, NettyServerRouter r) {
        switch (msg.getMsgType())
        {
            case LAYOUT_REQUEST:
                r.sendResponse(ctx, msg, new NettyLayoutResponseMsg(currentLayout));
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
