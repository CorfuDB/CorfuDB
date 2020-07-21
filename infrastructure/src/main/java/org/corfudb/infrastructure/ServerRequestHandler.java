package org.corfudb.infrastructure;


import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.client.RequestHandler;
import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;

@Slf4j
public class ServerRequestHandler extends RequestHandler {
    @Override
    protected void handlePing(Request request, ChannelHandlerContext ctx) {
        log.info("ServerRequestHandler[]: ping message received");
    }

    @Override
    protected void handleAuthenticate(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleBootstrap(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleCommitLayout(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleCommitTransaction(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleCompactLog(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleExecuteWorkFlow(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleFlash(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleGetLayout(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleGetToken(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleHealFailure(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handlePrepareLayout(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleProposeLayout(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleQueryLogMetadata(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleQueryNode(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleQueryStream(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleReadLog(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleReportFailure(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleRestart(Request request, ChannelHandlerContext ctx) {
        log.warn("[ServerRequestHandler]:Remote restart requested from client with " +
                "LSB: {} MSB:", request.getHeader().getClientId().getLsb(),request.getHeader().getClientId().getMsb());
        // TODO (Chetan): send ACK message back to the client?
        CorfuServer.restartServer(false);
    }

    @Override
    protected void handleSeal(Request request, ChannelHandlerContext ctx) {

    }

    @Override
    protected void handleTrimLog(Request request, ChannelHandlerContext ctx) {

    }
}