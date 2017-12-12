package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;

import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
public class NettyCorfuMessageDecoder extends LengthFieldBasedFrameDecoder {

    public NettyCorfuMessageDecoder() {
        super(Integer.MAX_VALUE, 0, 4,
            0, 4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf buf = (ByteBuf) super.decode(ctx, in);
        return buf == null ? null : CorfuMsg.deserialize(buf);
    }

}
