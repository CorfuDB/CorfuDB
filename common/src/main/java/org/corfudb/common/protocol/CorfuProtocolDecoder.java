package org.corfudb.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CorfuProtocolDecoder extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf msgBuf = (ByteBuf) msg;
        ByteBufInputStream msgInputStream = new ByteBufInputStream(msgBuf);

        try {



            if (log.isDebugEnabled()) {
                log.debug("Received msg {} from {}", cmd.getType(), ctx.channel().remoteAddress());
            }

        } finally {
            msgInputStream.close();
            msgBuf.release();
        }
    }
}
