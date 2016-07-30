package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuSetEpochMsg;
import org.corfudb.util.CorfuMsgHandler;

/**
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class BaseServer extends AbstractServer {

    /** Handler for the base server */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .addHandler(CorfuMsg.CorfuMsgType.PING, CorfuMsg.class, this::ping)
            .addHandler(CorfuMsg.CorfuMsgType.SET_EPOCH, CorfuSetEpochMsg.class, this::setEpoch);

    /** Respond to a ping message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    private void ping(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.PONG));
    }

    /** Respond to a epoch change message.
     *
     * @param csem      The incoming message
     * @param ctx       The channel context
     * @param r         The server router.
     */
    private void setEpoch(CorfuSetEpochMsg csem, ChannelHandlerContext ctx, IServerRouter r) {
        if (csem.getNewEpoch() >= r.getServerEpoch()) {
            log.info("Received SET_EPOCH, moving to new epoch {}", csem.getNewEpoch());
            r.setServerEpoch(csem.getNewEpoch());
            r.sendResponse(ctx, csem, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
        } else {
            log.debug("Rejected SET_EPOCH currrent={}, requested={}",
                    r.getServerEpoch(), csem.getNewEpoch());
            r.sendResponse(ctx, csem, new CorfuSetEpochMsg(CorfuMsg.CorfuMsgType.WRONG_EPOCH,
                    r.getServerEpoch()));
        }
    }

    @Override
    public void reset() {

    }
}
