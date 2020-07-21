package org.corfudb.common.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by Maithem on 7/1/20.
 */

@Slf4j
public abstract class ResponseHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf msgBuf = (ByteBuf) msg;
        msgBuf.readByte(); // Temporary -- Consume 0x2 marker
        ByteBufInputStream msgInputStream = new ByteBufInputStream(msgBuf);

        try {
            Response response = Response.parseFrom(msgInputStream);
            Header header = response.getHeader();

            if (log.isDebugEnabled()) {
                log.debug("Response {} pi {} from {}", header.getType(), ctx.channel().remoteAddress());
            }

            if (response.hasError()) {
                // propagate error to the client and return right away
                handleServerError(response);
                return;
            }

            // throw exceptions here?

            switch (header.getType()) {
                case PING:
                    checkArgument(response.hasPingResponse());
                    handlePing(response);
                    break;
                case RESTART:
                    checkArgument(response.hasRestartResponse());
                    handleRestart(response);
                    break;
                case RESET:
                    checkArgument(response.hasResetResponse());
                    handleReset(response);
                    break;
                case AUTHENTICATE:
                    checkArgument(response.hasAuthenticateResponse());
                    handleAuthenticate(response);
                    break;
                case SEAL:
                    checkArgument(response.hasSealResponse());
                    handleSeal(response);
                    break;
                case GET_LAYOUT:
                    checkArgument(response.hasGetLayoutResponse());
                    handleGetLayout(response);
                    break;
                case PREPARE_LAYOUT:
                    checkArgument(response.hasPrepareLayoutResponse());
                    handlePrepareLayout(response);
                    break;
                case PROPOSE_LAYOUT:
                    checkArgument(response.hasProposeLayoutResponse());
                    handleProposeLayout(response);
                    break;
                case COMMIT_LAYOUT:
                    checkArgument(response.hasCommitLayoutResponse());
                    handleCommitLayout(response);
                    break;
                case GET_TOKEN:
                    checkArgument(response.hasGetTokenResponse());
                    handleGetToken(response);
                    break;
                case COMMIT_TRANSACTION:
                    checkArgument(response.hasCommitTransactionResponse());
                    handleCommitTransaction(response);
                    break;
                case BOOTSTRAP:
                    checkArgument(response.hasBootstrapResponse());
                    handleBootstrap(response);
                    break;
                case QUERY_STREAM:
                    checkArgument(response.hasQueryStreamResponse());
                    handleQueryStream(response);
                    break;
                case READ_LOG:
                    checkArgument(response.hasReadLogResponse());
                    handleReadLog(response);
                    break;
                case QUERY_LOG_METADATA:
                    checkArgument(response.hasQueryLogMetadataResponse());
                    handleQueryLogMetadata(response);
                    break;
                case TRIM_LOG:
                    checkArgument(response.hasTrimLogResponse());
                    handleTrimLog(response);
                    break;
                case COMPACT_LOG:
                    checkArgument(response.hasCompactResponse());
                    handleCompactLog(response);
                    break;
                case FLASH:
                    checkArgument(response.hasFlashResponse());
                    handleFlash(response);
                    break;
                case QUERY_NODE:
                    checkArgument(response.hasQueryNodeResponse());
                    handleQueryNode(response);
                    break;
                case REPORT_FAILURE:
                    checkArgument(response.hasReportFailureResponse());
                    handleReportFailure(response);
                    break;
                case HEAL_FAILURE:
                    checkArgument(response.hasHealFailureResponse());
                    handleHealFailure(response);
                    break;
                case EXECUTE_WORKFLOW:
                    checkArgument(response.hasExecuteWorkflowResponse());
                    handleExecuteWorkFlow(response);
                    break;
                case UNRECOGNIZED:
                default:
                    // Clean exception? what does this message print?
                    log.error("Unknown message {}", response);
                    throw new UnsupportedOperationException();
            }
        } finally {
            msgInputStream.close();
            msgBuf.release();
        }
    }

    protected abstract void handleServerError(Response error);
    protected abstract void handlePing(Response response);
    protected abstract void handleRestart(Response response);
    protected abstract void handleReset(Response response);
    protected abstract void handleAuthenticate(Response response);
    protected abstract void handleSeal(Response response);
    protected abstract void handleGetLayout(Response response);
    protected abstract void handlePrepareLayout(Response response);
    protected abstract void handleProposeLayout(Response response);
    protected abstract void handleCommitLayout(Response response);
    protected abstract void handleGetToken(Response response);
    protected abstract void handleCommitTransaction(Response response);
    protected abstract void handleBootstrap(Response response);
    protected abstract void handleQueryStream(Response response);
    protected abstract void handleReadLog(Response response);
    protected abstract void handleQueryLogMetadata(Response response);
    protected abstract void handleTrimLog(Response response);
    protected abstract void handleCompactLog(Response response);
    protected abstract void handleFlash(Response response);
    protected abstract void handleQueryNode(Response response);
    protected abstract void handleReportFailure(Response response);
    protected abstract void handleHealFailure(Response response);
    protected abstract void handleExecuteWorkFlow(Response response);
}