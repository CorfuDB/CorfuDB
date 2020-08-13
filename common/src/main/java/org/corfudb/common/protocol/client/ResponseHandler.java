package org.corfudb.common.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.API;
import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by Maithem on 7/1/20.
 */

@Slf4j
public abstract class ResponseHandler {
    protected abstract void handleServerError(Response error);
    protected abstract void handlePing(Response response);
    protected abstract void handleRestart(Response response);
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