package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import java.lang.invoke.MethodHandles;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.JSONPayloadMsg;
import org.corfudb.protocols.wireprotocol.VersionInfo;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.Sleep;

/**
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class BaseServer extends AbstractServer {

    final ServerContext serverContext;

    private static final int RESET_ERROR_CODE = 100;
    private static final int RESTART_ERROR_CODE = 200;

    public BaseServer(@Nonnull ServerContext context) {
        this.serverContext = context;
    }

    /** Handler for the base server. */
    @Getter
    private final CorfuMsgHandler handler =
            CorfuMsgHandler.generateHandler(MethodHandles.lookup(), this);


    /** Respond to a ping message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type = CorfuMsgType.PING)
    private static void ping(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        r.sendResponse(ctx, msg, CorfuMsgType.PONG.msg());
    }

    /** Respond to a version request message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type = CorfuMsgType.VERSION_REQUEST)
    private void getVersion(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        VersionInfo vi = new VersionInfo(serverContext.getServerConfig(),
                                         serverContext.getNodeIdBase64());
        r.sendResponse(ctx, msg, new JSONPayloadMsg<>(vi, CorfuMsgType.VERSION_RESPONSE));
    }

    /**
     * Respond to a epoch change message.
     *
     * @param msg The incoming message
     * @param ctx The channel context
     * @param r   The server router.
     */
    @ServerHandler(type = CorfuMsgType.SET_EPOCH)
    public synchronized void handleMessageSetEpoch(@NonNull CorfuPayloadMsg<Long> msg,
                                                   ChannelHandlerContext ctx,
                                                   @NonNull IServerRouter r) {
        try {
            log.info("handleMessageSetEpoch: Received SET_EPOCH, moving to new epoch {}",
                    msg.getPayload());
            serverContext.setServerEpoch(msg.getPayload(), r);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        } catch (WrongEpochException e) {
            log.debug("handleMessageSetEpoch: Rejected SET_EPOCH current={}, requested={}",
                    e.getCorrectEpoch(), msg.getPayload());
            r.sendResponse(ctx, msg,
                    new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, e.getCorrectEpoch()));
        }
    }

    /** Reset the JVM. This mechanism leverages that corfu_server runs in a bash script
     * which monitors the exit code of Corfu. If the exit code is 100, then it resets
     * the server and DELETES ALL EXISTING DATA.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type = CorfuMsgType.RESET)
    private static void doReset(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.warn("Remote reset requested from client " + msg.getClientID());
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        System.exit(RESET_ERROR_CODE);
    }

    /** Restart the JVM. This mechanism leverages that corfu_server runs in a bash script
     * which monitors the exit code of Corfu. If the exit code is 200, then it restarts
     * the server.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type = CorfuMsgType.RESTART)
    private static void doRestart(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.warn("Remote restart requested from client " + msg.getClientID());
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        System.exit(RESTART_ERROR_CODE);
    }
}
