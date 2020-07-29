package org.corfudb.infrastructure;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.API;
import org.corfudb.common.protocol.client.RequestHandler;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;

@Slf4j
public class ServerRequestHandler extends RequestHandler {
    @Override
    protected void handlePing(Request request, ChannelHandlerContext ctx) {
        log.warn("handlePing: Remote reset requested from client with " +
                        "LSB: {} MSB:{}", request.getHeader().getClientId().getLsb(),
                request.getHeader().getClientId().getMsb());

        // send ResetResponse message back to the client
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
        // Temporary marker to indicate new Protobuf Message
        byteBuf.writeByte(0x2);
        byteBuf.writeBytes(API.newPingResponse(request.getHeader()).toByteArray());
        ctx.writeAndFlush(byteBuf);
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
    protected void handleReset(Request request, ChannelHandlerContext ctx) {
        log.warn("handleReset: Remote reset requested from client with " +
                "LSB: {} MSB:{}", request.getHeader().getClientId().getLsb(),
                request.getHeader().getClientId().getMsb());

        // send ResetResponse message back to the client
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
        // Temporary marker to indicate new Protobuf Message
        byteBuf.writeByte(0x2);
        byteBuf.writeBytes(API.newResetResponse(request.getHeader()).toByteArray());
        ctx.writeAndFlush(byteBuf);

        CorfuServer.restartServer(true);
    }

    @Override
    protected void handleRestart(Request request, ChannelHandlerContext ctx) {
        log.warn("handleRestart: Remote restart requested from client with " +
                "LSB: {} MSB:{}", request.getHeader().getClientId().getLsb(),
                request.getHeader().getClientId().getMsb());

        // send RestartResponse message back to the client
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
        // Temporary marker to indicate new Protobuf Message
        byteBuf.writeByte(0x2);
        byteBuf.writeBytes(API.newRestartResponse(request.getHeader()).toByteArray());
        ctx.writeAndFlush(byteBuf);

        CorfuServer.restartServer(false);
    }

    @Override
    protected void handleSeal(Request request, ChannelHandlerContext ctx) {
        log.warn("handleSeal: Received SEAL request from client with " +
                        "LSB: {} MSB:{}, " +
                        "moving to new epoch {}",
                request.getHeader().getClientId().getLsb(),
                request.getHeader().getClientId().getMsb(),
                request.getSealRequest().getEpoch());

        // TODO(Chetan): Complete the seal process

        // send SealResponse message back to the client
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
        // Temporary marker to indicate new Protobuf Message
        byteBuf.writeByte(0x2);
        byteBuf.writeBytes(API.newSealResponse(request.getHeader()).toByteArray());
        ctx.writeAndFlush(byteBuf);
    }

    @Override
    protected void handleTrimLog(Request request, ChannelHandlerContext ctx) {

    }
}