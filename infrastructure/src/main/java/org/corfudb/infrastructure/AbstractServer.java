package org.corfudb.infrastructure;

import com.codahale.metrics.Timer;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.MetricsUtils;

/**
 * Created by mwei on 12/4/15.
 */
@Slf4j
public abstract class AbstractServer {

    @Getter
    @Setter
    boolean shutdown;

    public AbstractServer() {
        shutdown = false;
    }

    /** Get the message handler for this instance.
     * @return  A message handler.
     */
    public abstract CorfuMsgHandler getHandler();

    /**
     * Handle a incoming Netty message.
     *
     * @param msg An incoming message.
     * @param ctx The channel handler context.
     * @param r   The router that took in the message.
     */
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        Timer.Context context = null;

        if (isShutdown()) return;
        switch (msg.getMsgType()) {
            case PING:
                context = MetricsUtils.getConditionalContext(BaseServer.getTimerPing());
                break;
            case VERSION_REQUEST:
                context = MetricsUtils.getConditionalContext(BaseServer.getTimerVersionRequest());
                break;
            case WRITE:
                context = MetricsUtils.getConditionalContext(LogUnitServer.getTimerLogWrite());
                break;
            case READ_REQUEST:
                context = MetricsUtils.getConditionalContext(LogUnitServer.getTimerLogRead());
                break;
            case TRIM:
                context = MetricsUtils.getConditionalContext(LogUnitServer.getTimerLogTrim());
                break;
            case FILL_HOLE:
                context = MetricsUtils.getConditionalContext(LogUnitServer.getTimerLogFillHole());
                break;
            case FORCE_GC:
                context = MetricsUtils.getConditionalContext(LogUnitServer.getTimerLogForceGc());
                break;
            case GC_INTERVAL:
                context = MetricsUtils.getConditionalContext(LogUnitServer.getTimerLogGcInterval());
                break;
            case COMMIT:
                context = MetricsUtils.getConditionalContext(LogUnitServer.getTimerLogCommit());
                break;
            case LAYOUT_REQUEST:
                context = MetricsUtils.getConditionalContext(LayoutServer.getTimerLayoutReq());
                break;
            case LAYOUT_PREPARE:
                context = MetricsUtils.getConditionalContext(LayoutServer.getTimerLayoutPrepare());
                break;
            case LAYOUT_PROPOSE:
                context = MetricsUtils.getConditionalContext(LayoutServer.getTimerLayoutPropose());
                break;
            case LAYOUT_COMMITTED:
                context = MetricsUtils.getConditionalContext(LayoutServer.getTimerLayoutCommitted());
                break;
            case LAYOUT_BOOTSTRAP:
                context = MetricsUtils.getConditionalContext(LayoutServer.getTimerLayoutBootstrap());
                break;
            case SET_EPOCH:
                context = MetricsUtils.getConditionalContext(LayoutServer.getTimerLayoutSetEpoch());
                break;
            case TOKEN_REQ:
                context = MetricsUtils.getConditionalContext(SequencerServer.getTimerSeqReq());
                break;
            default:
                break;
        }
        if (!getHandler().handle(msg, ctx, r)) {
            log.warn("Received unhandled message type {}" , msg.getMsgType());
        }
        MetricsUtils.stopConditionalContext(context);
    }

    /**
     * Shutdown the server.
     */
    public void shutdown() {
        setShutdown(true);
    }

}
