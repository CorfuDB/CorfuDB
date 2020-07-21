package org.corfudb.common.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by Maithem on 7/1/20.
 */

@Slf4j
public abstract class RequestHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf msgBuf = (ByteBuf) msg;

        // Temporary -- Check Corfu msg marker: 0x1 indicates legacy while 0x2 indicates new
        if(msgBuf.getByte(msgBuf.readerIndex()) == 0x1) {
            ctx.fireChannelRead(msgBuf); // Forward legacy corfu msg to next handler
            return;
        }

        msgBuf.readByte(); // Temporary -- Consume 0x2 marker

        ByteBufInputStream msgInputStream = new ByteBufInputStream(msgBuf);
        
        try {
            Request request = Request.parseFrom(msgInputStream);
            Header  header = request.getHeader();

            if (log.isDebugEnabled()) {
                log.debug("Request {} pi {} from {}", header.getType(), ctx.channel().remoteAddress());
            }

            // drop messages with bad header verification?
            // check if rpc has exception

            switch (header.getType()) {
                case PING:
                    checkArgument(request.hasPingRequest());
                    handlePing(request, ctx);
                    break;
                case RESTART:
                    checkArgument(request.hasRestartRequest());
                    handleRestart(request, ctx);
                    break;
                case RESET:
                    checkArgument(request.hasResetRequest());
                    handleReset(request, ctx);
                    break;
                case AUTHENTICATE:
                    checkArgument(request.hasAuthenticateRequest());
                    handleAuthenticate(request, ctx);
                    break;
                case SEAL:
                    checkArgument(request.hasSealRequest());
                    handleSeal(request, ctx);
                    break;
                case GET_LAYOUT:
                    checkArgument(request.hasGetLayoutRequest());
                    handleGetLayout(request, ctx);
                    break;
                case PREPARE_LAYOUT:
                    checkArgument(request.hasPrepareLayoutRequest());
                    handlePrepareLayout(request, ctx);
                    break;
                case PROPOSE_LAYOUT:
                    checkArgument(request.hasProposeLayoutRequest());
                    handleProposeLayout(request, ctx);
                    break;
                case COMMIT_LAYOUT:
                    checkArgument(request.hasCommitLayoutRequest());
                    handleCommitLayout(request, ctx);
                    break;
                case GET_TOKEN:
                    checkArgument(request.hasGetTokenRequest());
                    handleGetToken(request, ctx);
                    break;
                case COMMIT_TRANSACTION:
                    checkArgument(request.hasCommitTransactionRequest());
                    handleCommitTransaction(request, ctx);
                    break;
                case BOOTSTRAP:
                    checkArgument(request.hasBootstrapRequest());
                    handleBootstrap(request, ctx);
                    break;
                case QUERY_STREAM:
                    checkArgument(request.hasQueryStreamRequest());
                    handleQueryStream(request, ctx);
                    break;
                case READ_LOG:
                    checkArgument(request.hasReadLogRequest());
                    handleReadLog(request, ctx);
                    break;
                case QUERY_LOG_METADATA:
                    checkArgument(request.hasQueryLogMetadataRequest());
                    handleQueryLogMetadata(request, ctx);
                    break;
                case TRIM_LOG:
                    checkArgument(request.hasTrimLogRequest());
                    handleTrimLog(request, ctx);
                    break;
                case COMPACT_LOG:
                    checkArgument(request.hasCompactRequest());
                    handleCompactLog(request, ctx);
                    break;
                case FLASH:
                    checkArgument(request.hasFlashRequest());
                    handleFlash(request, ctx);
                    break;
                case QUERY_NODE:
                    checkArgument(request.hasQueryNodeRequest());
                    handleQueryNode(request, ctx);
                    break;
                case REPORT_FAILURE:
                    checkArgument(request.hasReportFailureRequest());
                    handleReportFailure(request, ctx);
                    break;
                case HEAL_FAILURE:
                    checkArgument(request.hasReportFailureRequest());
                    handleHealFailure(request, ctx);
                    break;
                case EXECUTE_WORKFLOW:
                    checkArgument(request.hasExecuteWorkflowRequest());
                    handleExecuteWorkFlow(request, ctx);
                    break;
                case UNRECOGNIZED:
                default:
                    // Clean exception? what does this message print?
                    log.error("Unknown message {}", request);
                    throw new UnsupportedOperationException();
            }
        } finally {
            msgInputStream.close();
            msgBuf.release();
        }
    }

    protected abstract void handlePing(Request request, ChannelHandlerContext ctx);
    protected abstract void handleRestart(Request request, ChannelHandlerContext ctx);
    protected abstract void handleReset(Request request, ChannelHandlerContext ctx);
    protected abstract void handleAuthenticate(Request request, ChannelHandlerContext ctx);
    protected abstract void handleSeal(Request request, ChannelHandlerContext ctx);
    protected abstract void handleGetLayout(Request request, ChannelHandlerContext ctx);
    protected abstract void handlePrepareLayout(Request request, ChannelHandlerContext ctx);
    protected abstract void handleProposeLayout(Request request, ChannelHandlerContext ctx);
    protected abstract void handleCommitLayout(Request request, ChannelHandlerContext ctx);
    protected abstract void handleGetToken(Request request, ChannelHandlerContext ctx);
    protected abstract void handleCommitTransaction(Request request, ChannelHandlerContext ctx);
    protected abstract void handleBootstrap(Request request, ChannelHandlerContext ctx);
    protected abstract void handleQueryStream(Request request, ChannelHandlerContext ctx);
    protected abstract void handleReadLog(Request request, ChannelHandlerContext ctx);
    protected abstract void handleQueryLogMetadata(Request request, ChannelHandlerContext ctx);
    protected abstract void handleTrimLog(Request request, ChannelHandlerContext ctx);
    protected abstract void handleCompactLog(Request request, ChannelHandlerContext ctx);
    protected abstract void handleFlash(Request request, ChannelHandlerContext ctx);
    protected abstract void handleQueryNode(Request request, ChannelHandlerContext ctx);
    protected abstract void handleReportFailure(Request request, ChannelHandlerContext ctx);
    protected abstract void handleHealFailure(Request request, ChannelHandlerContext ctx);
    protected abstract void handleExecuteWorkFlow(Request request, ChannelHandlerContext ctx);
}
