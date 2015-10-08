package org.corfudb.infrastructure.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
public class NettyCorfuMessageEncoder extends MessageToByteEncoder<NettyCorfuMsg> {

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext,
                          NettyCorfuMsg nettyCorfuMsg,
                          ByteBuf byteBuf) throws Exception {
        try {
            nettyCorfuMsg.serialize(byteBuf);
        } catch (Exception e)
        {
            log.error("Error during serialization!", e);
        }
    }
}
