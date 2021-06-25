package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;

import static org.corfudb.protocols.CorfuProtocolServerErrors.getWrongEpochErrorMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getPingResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getResetResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getRestartResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getSealResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;

/**
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class BaseServer extends AbstractServer {

    public final ServerContext serverContext;

    private final ExecutorService executor;

    /**
     * RequestHandlerMethods for the Base server
     */
    @Getter
    private final RequestHandlerMethods handlerMethods =
            RequestHandlerMethods.generateHandler(MethodHandles.lookup(), this);

    public BaseServer(@Nonnull ServerContext context) {
        serverContext = context;
        executor = serverContext.getExecutorService(serverContext.getConfiguration().getNumBaseServerThreads(), "baseServer-");
    }

    @Override
    protected void processRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        executor.submit(() -> getHandlerMethods().handle(req, ctx, r));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
    }

    /**
     * Respond to a ping request.
     *
     * @param req   The incoming request message.
     * @param ctx   The channel context.
     * @param r     The server router.
     */
    @RequestHandler(type = RequestPayloadMsg.PayloadCase.PING_REQUEST)
    public void handlePing(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("handlePing[{}]: Ping message received from {} {}", req.getHeader().getRequestId(),
                req.getHeader().getClientId().getMsb(), req.getHeader().getClientId().getLsb());

        HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        ResponseMsg response = getResponseMsg(responseHeader, getPingResponseMsg());
        r.sendResponse(response, ctx);
    }

    /**
     * Respond to a epoch change request.
     * This method also executes sealing logic on each individual server type.
     *
     * @param req The incoming request message.
     * @param ctx The channel context.
     * @param r The server router.
     */
    @RequestHandler(type = RequestPayloadMsg.PayloadCase.SEAL_REQUEST)
    private synchronized void handleSeal(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        try {
            final long epoch = req.getPayload().getSealRequest().getEpoch();
            String remoteHostAddress;
            try {
                remoteHostAddress = ((InetSocketAddress)ctx.channel().remoteAddress()).getAddress().getHostAddress();
            } catch (NullPointerException ex) {
                remoteHostAddress = "unavailable";
            }

            log.info("handleSeal[{}]: Received SEAL from (clientId={}:{}), moving to new epoch {},",
                    req.getHeader().getRequestId(), req.getHeader().getClientId(), remoteHostAddress, epoch);

            serverContext.setServerEpoch(epoch, r);
            HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            ResponseMsg response = getResponseMsg(responseHeader, getSealResponseMsg());
            r.sendResponse(response, ctx);
        } catch (WrongEpochException e) {
            log.debug("handleSeal[{}]: Rejected SEAL current={}, requested={}", req.getHeader().getRequestId(),
                    e.getCorrectEpoch(), req.getPayload().getSealRequest().getEpoch());

            HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            ResponseMsg response = getResponseMsg(responseHeader, getWrongEpochErrorMsg(e.getCorrectEpoch()));
            r.sendResponse(response, ctx);
        }
    }

    /**
     * Reset the JVM. This mechanism leverages that corfu_server runs in a bash script
     * which monitors the exit code of Corfu. If the exit code is 100, then it resets
     * the server and DELETES ALL EXISTING DATA.
     *
     * @param req The incoming request message.
     * @param ctx The channel context.
     * @param r The server router.
     */
    @RequestHandler(type = RequestPayloadMsg.PayloadCase.RESET_REQUEST)
    private void handleReset(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        log.warn("handleReset[{}]: Remote reset requested from client {}",
                req.getHeader().getRequestId(), req.getHeader().getClientId());

        HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        ResponseMsg response = getResponseMsg(responseHeader, getResetResponseMsg());
        r.sendResponse(response, ctx);
        CorfuServer.restartServer(true);
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
    @RequestHandler(type = RequestPayloadMsg.PayloadCase.RESTART_REQUEST)
    private void handleRestart(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        log.warn("handleRestart[{}]: Remote restart requested from client {}",
                req.getHeader().getRequestId(), req.getHeader().getClientId());

        HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        ResponseMsg response = getResponseMsg(responseHeader, getRestartResponseMsg());
        r.sendResponse(response, ctx);
        CorfuServer.restartServer(false);
    }
}
