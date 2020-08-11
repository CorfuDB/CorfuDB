package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.API;
import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;
import org.corfudb.infrastructure.RequestHandler;
import org.corfudb.infrastructure.RequestHandlerMethods;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.JSONPayloadMsg;
import org.corfudb.protocols.wireprotocol.VersionInfo;
import org.corfudb.runtime.exceptions.WrongEpochException;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class BaseServer extends AbstractServer {

    public final ServerContext serverContext;

    private final ExecutorService executor;

    /** HandlerMethod for the base server. */
    @Getter
    private final HandlerMethods handler = HandlerMethods.generateHandler(MethodHandles.lookup(), this);

    /** RequestHandlerMethods for the base server. */
    private final RequestHandlerMethods handlerMethods = RequestHandlerMethods.generateHandler(MethodHandles.lookup(), this);

    @Override
    public RequestHandlerMethods getHandlerMethods() { return handlerMethods; }

    @Override
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        return getState() == ServerState.READY;
    }

    @Override
    public boolean isServerReadyToHandleReq(Header requestHeader) {
        return getState() == ServerState.READY;
    }

    public BaseServer(@Nonnull ServerContext context) {
        this.serverContext = context;
        executor = Executors.newFixedThreadPool(serverContext.getBaseServerThreadCount(),
                new ServerThreadFactory("baseServer-", new ServerThreadFactory.ExceptionHandler()));
    }

    @Override
    protected void processRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        executor.submit(() -> getHandler().handle(msg, ctx, r));
    }

    @Override
    protected void processRequest(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        executor.submit(() -> getHandlerMethods().handle(req, ctx, r));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
    }

    /**
     * Respond to a ping message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type = CorfuMsgType.PING)
    private void ping(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        r.sendResponse(ctx, msg, CorfuMsgType.PONG.msg());
    }

    /**
     * Respond to a ping request.
     *
     * @param req   The incoming request message.
     * @param ctx   The channel context.
     * @param r     The server router.
     */
    @RequestHandler(type = CorfuProtocol.MessageType.PING)
    private void handlePing(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        log.info("handlePing[{}]: Ping message received from {} {}", req.getHeader().getRequestId(),
                req.getHeader().getClientId().getMsb(), req.getHeader().getClientId().getLsb());

        Header responseHeader = API.generateResponseHeader(req.getHeader(), false, true);
        Response response = API.getPingResponse(responseHeader);
        r.sendResponse(response, ctx);
    }

    /**
     * Respond to a keep alive message.
     * Note: this message ignores epoch.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type = CorfuMsgType.KEEP_ALIVE)
    private void keepAlive(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    /**
     * Respond to a version request message.
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
     * This method also executes sealing logic on each individual server type.
     *
     * @param msg The incoming message
     * @param ctx The channel context
     * @param r   The server router.
     */
    @ServerHandler(type = CorfuMsgType.SEAL)
    public synchronized void handleMessageSetEpoch(@NonNull CorfuPayloadMsg<Long> msg,
                                                   ChannelHandlerContext ctx,
                                                   @NonNull IServerRouter r) {
        try {
            long epoch = msg.getPayload();
            String remoteHostAddress;
            try {
                remoteHostAddress = ((InetSocketAddress)ctx.channel().remoteAddress()).getAddress().getHostAddress();
            } catch (NullPointerException e) {
                remoteHostAddress = "unavailable";
            }
            log.info("handleMessageSetEpoch: Received SEAL from (clientId={}:{}), moving to new epoch {}",
                    msg.getClientID(), remoteHostAddress, epoch);
            serverContext.setServerEpoch(epoch, r);
            r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        } catch (WrongEpochException e) {
            log.debug("handleMessageSetEpoch: Rejected SEAL current={}, requested={}",
                    e.getCorrectEpoch(), msg.getPayload());
            r.sendResponse(ctx, msg,
                    new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, e.getCorrectEpoch()));
        }
    }

    /**
     * Respond to a epoch change request.
     * This method also executes sealing logic on each individual server type.
     * @param req The incoming request message.
     * @param ctx The channel context.
     * @param r The server router.
     */
    @RequestHandler(type = CorfuProtocol.MessageType.SEAL)
    private synchronized void handleSeal(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        try {
            long epoch = req.getSealRequest().getEpoch();
            String remoteHostAddress;
            try {
                remoteHostAddress = ((InetSocketAddress)ctx.channel().remoteAddress()).getAddress().getHostAddress();
            } catch(NullPointerException ex) {
                remoteHostAddress = "unavailable";
            }

            log.info("handleSeal[{}]: Received SEAL from (clientId={}:{}), moving to new epoch {},",
                    req.getHeader().getRequestId(), req.getHeader().getClientId(), remoteHostAddress, epoch);

            // TODO(Zach):
            // serverContext.setServerEpoch(epoch, r);
            Header responseHeader = API.generateResponseHeader(req.getHeader(), false, true);
            Response response = API.getSealResponse(responseHeader);
            r.sendResponse(response, ctx);
        } catch (WrongEpochException e) {
            log.debug("handleSeal[{}]: Rejected SEAL current={}, requested={}",
                    req.getHeader().getRequestId(), e.getCorrectEpoch(), req.getSealRequest().getEpoch());

            r.sendWrongEpochError(req.getHeader(), ctx);
        }
    }

    /**
     * Reset the JVM. This mechanism leverages that corfu_server runs in a bash script
     * which monitors the exit code of Corfu. If the exit code is 100, then it resets
     * the server and DELETES ALL EXISTING DATA.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type = CorfuMsgType.RESET)
    private void doReset(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.warn("Remote reset requested from client {}", msg.getClientID());
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        CorfuServer.restartServer(true);
    }

    /**
     * Reset the JVM. This mechanism leverages that corfu_server runs in a bash script
     * which monitors the exit code of Corfu. If the exit code is 100, then it resets
     * the server and DELETES ALL EXISTING DATA.
     * @param req The incoming request message.
     * @param ctx The channel context.
     * @param r The server router.
     */
    @RequestHandler(type = CorfuProtocol.MessageType.RESET)
    private void handleReset(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        log.warn("handleReset[{}]: Remote reset requested from client {}",
                req.getHeader().getRequestId(), req.getHeader().getClientId());

        Header responseHeader = API.generateResponseHeader(req.getHeader(), false, true);
        Response response = API.getResetResponse(responseHeader);
        r.sendResponse(response, ctx);
        CorfuServer.restartServer(true);
    }

    /**
     * Restart the JVM. This mechanism leverages that corfu_server runs in a bash script
     * which monitors the exit code of Corfu. If the exit code is 200, then it restarts
     * the server.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type = CorfuMsgType.RESTART)
    private void doRestart(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.warn("Remote restart requested from client {}", msg.getClientID());
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
        CorfuServer.restartServer(false);
    }

    /**
     * Restart the JVM. This mechanism leverages that corfu_server runs in a bash script
     * which monitors the exit code of Corfu. If the exit code is 200, then it restarts
     * the server.
     *
     * @param req   The incoming request message.
     * @param ctx   The channel context.
     * @param r     The server router.
     */
    @RequestHandler(type = CorfuProtocol.MessageType.RESTART)
    private void handleRestart(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        log.warn("handleRestart[{}]: Remote restart requested from client {}",
                req.getHeader().getRequestId(), req.getHeader().getClientId());

        Header responseHeader = API.generateResponseHeader(req.getHeader(), false, true);
        Response response = API.getRestartResponse(responseHeader);
        r.sendResponse(response, ctx);
        CorfuServer.restartServer(false);
    }
}
