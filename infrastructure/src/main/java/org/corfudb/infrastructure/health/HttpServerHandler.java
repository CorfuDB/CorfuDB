package org.corfudb.infrastructure.health;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.CharsetUtil;
import lombok.extern.slf4j.Slf4j;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Slf4j
public class HttpServerHandler extends SimpleChannelInboundHandler<HttpObject> {


    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;
            boolean keepAlive = HttpUtil.isKeepAlive(req);
            FullHttpResponse response;
            if (req.uri().equals("/health")) {
                if (!HealthMonitor.isInit()) {
                    log.info("Health not initialized");
                    response = new DefaultFullHttpResponse(req.protocolVersion(), BAD_REQUEST,
                            Unpooled.copiedBuffer("Health monitor is not initialized", CharsetUtil.UTF_8));
                    response.headers()
                            .set(CONTENT_TYPE, TEXT_PLAIN);
                }
                else{
                    final HealthReport healthReport = HealthMonitor.generateHealthReport();
                    log.info("Generating report: ");
                    log.info(healthReport.asJson());
                    response = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                            Unpooled.copiedBuffer(healthReport.asJson(), CharsetUtil.UTF_8));
                    response.headers()
                            .set(CONTENT_TYPE, "application/json");
                }
            } else {
                log.info("Bad request: ");
                response = new DefaultFullHttpResponse(req.protocolVersion(), BAD_REQUEST,
                        Unpooled.copiedBuffer("Bad request", CharsetUtil.UTF_8));
                response.headers()
                        .set(CONTENT_TYPE, TEXT_PLAIN);
            }
            response.headers()
                    .setInt(CONTENT_LENGTH, response.content().readableBytes());
            if (keepAlive) {
                if (!req.protocolVersion().isKeepAliveDefault()) {
                    response.headers().set(CONNECTION, KEEP_ALIVE);
                }
            } else {
                response.headers().set(CONNECTION, CLOSE);
            }
            ChannelFuture f = ctx.write(response);
            if (!keepAlive) {
                f.addListener(ChannelFutureListener.CLOSE);
            }
            ctx.flush();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}
