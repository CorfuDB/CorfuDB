package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.JSONPayloadMsg;
import org.corfudb.protocols.wireprotocol.VersionInfo;
import org.corfudb.util.Utils;

/**
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class BaseServer extends AbstractServer {

    /** Options map, if available. */
    @Getter
    @Setter
    public Map<String, Object> optionsMap = new HashMap<>();

    /** Handler for the base server. */
    @Getter
    private final CorfuMsgHandler handler = new CorfuMsgHandler()
            .generateHandlers(MethodHandles.lookup(), this);

    private static final String metricsPrefix = "corfu.server.base.";

    /** Respond to a ping message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type = CorfuMsgType.PING, opTimer = metricsPrefix + "ping")
    private static void ping(CorfuMsg msg, ChannelHandlerContext ctx,
                             IServerRouter r, boolean isMetricsEnabled) {
        r.sendResponse(ctx, msg, CorfuMsgType.PONG.msg());
    }

    /** Respond to a version request message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type = CorfuMsgType.VERSION_REQUEST, opTimer = metricsPrefix + "version-request")
    private void getVersion(CorfuMsg msg, ChannelHandlerContext ctx,
                            IServerRouter r, boolean isMetricsEnabled) {
        VersionInfo vi = new VersionInfo(optionsMap);
        r.sendResponse(ctx, msg, new JSONPayloadMsg<>(vi, CorfuMsgType.VERSION_RESPONSE));
    }

    /** Reset the JVM. This mechanism leverages that corfu_server runs in a bash script
     * which monitors the exit code of Corfu. If the exit code is 100, then it restarts
     * the server.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type = CorfuMsgType.RESET)
    private static void doReset(CorfuMsg msg, ChannelHandlerContext ctx,
                                IServerRouter r, boolean isMetricsEnabled) {
        log.warn("Remote reset requested from client " + msg.getClientID());
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        Utils.sleepUninterruptibly(500); // Sleep, to make sure that all channels are flushed...
        System.exit(100);
    }
}
