package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

import lombok.extern.slf4j.Slf4j;


/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
public class NettyCorfuMessageDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf,
                          List<Object> list) throws Exception {
        list.add(CorfuMsg.deserialize(byteBuf));
    }
}
