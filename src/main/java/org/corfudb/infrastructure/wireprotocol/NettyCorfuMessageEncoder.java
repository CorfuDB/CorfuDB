package org.corfudb.infrastructure.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by mwei on 10/1/15.
 */
public class NettyCorfuMessageEncoder extends MessageToByteEncoder<NettyCorfuMsg> {

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext,
                          NettyCorfuMsg nettyCorfuMsg,
                          ByteBuf byteBuf) throws Exception {
        nettyCorfuMsg.serialize(byteBuf);
    }
}
