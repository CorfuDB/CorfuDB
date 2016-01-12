package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuSetEpochMsg;

/**
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class BaseServer implements IServer {

    IServerRouter router;

    public BaseServer(IServerRouter router)
    {
        this.router = router;
    }

    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        switch (msg.getMsgType())
        {
            case PING:
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.PONG));
                break;
            case SET_EPOCH:
                CorfuSetEpochMsg csem = (CorfuSetEpochMsg) msg;
                if (csem.getNewEpoch() >= router.getEpoch())
                {
                    log.info("Received SET_EPOCH, moving to new epoch {}", csem.getNewEpoch());
                    router.setEpoch(csem.getNewEpoch());
                    router.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
                }
                else
                {
                    log.debug("Rejected SET_EPOCH currrent={}, requested={}",
                            router.getEpoch(), csem.getNewEpoch());
                    router.sendResponse(ctx, msg, new CorfuSetEpochMsg(CorfuMsg.CorfuMsgType.WRONG_EPOCH,
                            router.getEpoch()));
                }
        }
    }

    @Override
    public void reset() {

    }
}
