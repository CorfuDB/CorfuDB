package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import lombok.extern.slf4j.Slf4j;


/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
public class NettyCorfuMessageDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf,
                          List<Object> list) throws Exception {
        list.add(CorfuMsg.deserialize(byteBuf));
    }

    @Override
    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in,
                              List<Object> out) throws Exception {
        //log.info("Netty channel handler context goes inactive, received out size is {}",
        // (out == null) ? null : out.size());

        if (in != Unpooled.EMPTY_BUFFER) {
            this.decode(ctx, in, out);
        }
        // ignore the Netty generated {@link EmptyByteBuf empty ByteBuf message} when channel
        // handler goes inactive (typically happened after each received burst of batch of messages)
    }
}
