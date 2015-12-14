package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;

/**
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class BaseServer implements IServer {

    NettyServerRouter router;

    public BaseServer(NettyServerRouter router)
    {
        this.router = router;
    }

    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        switch (msg.getMsgType())
        {
            case PING:
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.NettyCorfuMsgType.PONG));
                break;
        }
    }

    @Override
    public void reset() {

    }
}
