package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;

import static org.corfudb.common.util.URLUtils.getRemoteEndpointFromCtx;
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
        executor = serverContext.getExecutorService(serverContext.getBaseServerThreadCount(), "baseServer-");
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
        log.trace("handlePing[{}]: Ping message received from {}",
                req.getHeader().getRequestId(), getRemoteEndpointFromCtx(ctx));

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

            log.info("handleSeal[{}]: Received SEAL from {}, moving to new epoch {},",
                    req.getHeader().getRequestId(), getRemoteEndpointFromCtx(ctx), epoch);

            serverContext.setServerEpoch(epoch, r);
            HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            ResponseMsg response = getResponseMsg(responseHeader, getSealResponseMsg());
            r.sendResponse(response, ctx);
        } catch (WrongEpochException e) {
            log.debug("handleSeal[{}]: Rejected SEAL from {} current={}, requested={}",
                    req.getHeader().getRequestId(), getRemoteEndpointFromCtx(ctx),
                    e.getCorrectEpoch(), req.getPayload().getSealRequest().getEpoch());

            HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
            ResponseMsg response = getResponseMsg(responseHeader, getWrongEpochErrorMsg(e.getCorrectEpoch()));
            r.sendResponse(response, ctx);
        }
    }

    /**
     * Restart the CorfuServer and reset the server state by DELETING ALL EXISTING DATA.
     *
     * @param req The incoming request message.
     * @param ctx The channel context.
     * @param r The server router.
     */
    @RequestHandler(type = RequestPayloadMsg.PayloadCase.RESET_REQUEST)
    private void handleReset(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        log.warn("handleReset[{}]: Remote reset requested from {}",
                req.getHeader().getRequestId(), getRemoteEndpointFromCtx(ctx));

        HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        ResponseMsg response = getResponseMsg(responseHeader, getResetResponseMsg());
        r.sendResponse(response, ctx);
        CorfuServer.restartServer(true);
    }

    /**
     * Restart the CorfuServer. Do NOT reset any of the server state.
     *
     * @param req   The incoming request message.
     * @param ctx   The channel context.
     * @param r     The server router.
     */
    @RequestHandler(type = RequestPayloadMsg.PayloadCase.RESTART_REQUEST)
    private void handleRestart(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        log.warn("handleRestart[{}]: Remote restart requested from {}",
                req.getHeader().getRequestId(), getRemoteEndpointFromCtx(ctx));

        HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        ResponseMsg response = getResponseMsg(responseHeader, getRestartResponseMsg());
        r.sendResponse(response, ctx);
        CorfuServer.restartServer(false);
    }
}
