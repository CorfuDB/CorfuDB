package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuSetEpochMsg;
import org.corfudb.protocols.wireprotocol.JSONPayloadMsg;
import org.corfudb.protocols.wireprotocol.VersionInfo;
import org.corfudb.util.CorfuMsgHandler;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class BaseServer implements IServer {

    /** Options map, if available */
    @Getter
    @Setter
    public Map<String, Object> optionsMap = new HashMap<>();

    /** Handler for the base server */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .addHandler(CorfuMsg.CorfuMsgType.PING, BaseServer::ping)
            .addHandler(CorfuMsg.CorfuMsgType.SET_EPOCH, BaseServer::setEpoch)
            .addHandler(CorfuMsg.CorfuMsgType.VERSION_REQUEST, this::getVersion);

    /** Respond to a ping message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    private static void ping(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.PONG));
    }

    /** Respond to a epoch change message.
     *
     * @param csem      The incoming message
     * @param ctx       The channel context
     * @param r         The server router.
     */
    private static void setEpoch(CorfuSetEpochMsg csem, ChannelHandlerContext ctx, IServerRouter r) {
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

    /** Respond to a version request message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    private void getVersion(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        VersionInfo vi = new VersionInfo(optionsMap);
        r.sendResponse(ctx, msg, new JSONPayloadMsg<>(vi, CorfuMsg.CorfuMsgType.VERSION_RESPONSE));
    }

    @Override
    public void reset() {

    }
}
