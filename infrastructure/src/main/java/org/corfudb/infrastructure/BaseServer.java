package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerThreadFactory.ExceptionHandler;
import org.corfudb.infrastructure.server.CorfuServerStateGraph.CorfuServerState;
import org.corfudb.infrastructure.server.CorfuServerStateMachine;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.JSONPayloadMsg;
import org.corfudb.protocols.wireprotocol.VersionInfo;
import org.corfudb.runtime.exceptions.WrongEpochException;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class BaseServer extends AbstractServer {

    final ServerContext serverContext;

    private final ExecutorService executor;
    private final CorfuServerStateMachine serverStateMachine;

    /** HandlerMethod for the base server. */
    @Getter
    private final HandlerMethods handler = HandlerMethods.generateHandler(MethodHandles.lookup(), this);

    @Override
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        return getState() == ServerState.READY;
    }

    public BaseServer(@Nonnull ServerContext context, CorfuServerStateMachine serverStateMachine) {
        this.serverContext = context;
        this.serverStateMachine = serverStateMachine;
        executor = Executors.newFixedThreadPool(serverContext.getBaseServerThreadCount(),
                new ServerThreadFactory("baseServer-", new ExceptionHandler()));
    }

    @Override
    protected void processRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        executor.submit(() -> getHandler().handle(msg, ctx, r));
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        markShutdown();
        return shutdownServerExecutor(executor);
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
     * Reset the JVM. This mechanism leverages that corfu_server runs in a bash script
     * which monitors the exit code of Corfu. If the exit code is 100, then it resets
     * the server and DELETES ALL EXISTING DATA.
     *
     * @param msg    The incoming message
     * @param ctx    The channel context
     * @param router The server router.
     */
    @ServerHandler(type = CorfuMsgType.RESET)
    private void doReset(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter router) {
        log.warn("Remote reset requested from client {}", msg.getClientID());
        serverStateMachine
                .next(CorfuServerState.STOP_AND_CLEAN)
                .thenRun(() -> router.sendResponse(ctx, msg, CorfuMsgType.ACK.msg()));
    }

    /**
     * Restart the JVM. This mechanism leverages that corfu_server runs in a bash script
     * which monitors the exit code of Corfu. If the exit code is 200, then it restarts
     * the server.
     *
     * @param msg    The incoming message
     * @param ctx    The channel context
     * @param router The server router.
     */
    @ServerHandler(type = CorfuMsgType.RESTART)
    private void doRestart(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter router) {
        log.warn("Remote restart requested from client {}", msg.getClientID());
        serverStateMachine
                .next(CorfuServerState.STOP)
                .thenRun(() -> router.sendResponse(ctx, msg, CorfuMsgType.ACK.msg()));
    }
}
