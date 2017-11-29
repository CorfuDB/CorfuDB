package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;

import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;

import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
@Sharable
public class NettyCorfuMessageDecoder extends MessageToMessageDecoder<ByteBuf> {


    protected void decode(@Nonnull ChannelHandlerContext channelHandlerContext,
                          @Nonnull ByteBuf byteBuf,
                          @Nonnull List<Object> list) throws Exception {
        list.add(CorfuMsg.deserialize(byteBuf));
    }

}
