package org.corfudb.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;

/**
 * Created by Maithem on 7/1/20.
 */

@Slf4j
public class ResponseHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf msgBuf = (ByteBuf) msg;
        ByteBufInputStream msgInputStream = new ByteBufInputStream(msgBuf);

        try {
            Response request = Response.parseFrom(msgInputStream);
            Header header = request.getHeader();

            if (log.isDebugEnabled()) {
                log.debug("Received {} pi {} from {}", header.getType(), ctx.channel().remoteAddress());
            }

            switch (header.getType()) {

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
}