package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.wireprotocol.NettyCorfuMsg;

/**
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class BaseNettyServer implements INettyServer {

    NettyServerRouter router;

    public BaseNettyServer(NettyServerRouter router)
    {
        this.router = router;
    }

    @Override
    public void handleMessage(NettyCorfuMsg msg, ChannelHandlerContext ctx, NettyServerRouter r) {
        switch (msg.getMsgType())
        {
            case PING:
                r.sendResponse(ctx, msg, new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.PONG));
                break;
        }
    }

    @Override
    public void reset() {

    }
}
