package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
@Sharable
public class NettyCorfuMessageEncoder extends MessageToMessageEncoder<CorfuMsg> {

    @Override
    protected void encode(
        @Nonnull ChannelHandlerContext ctx,
        @Nonnull CorfuMsg corfuMsg,
        @Nonnull List<Object> list) throws Exception {
        // Nice if the size could be known
        final ByteBuf buffer = ctx.alloc().directBuffer();
        try {
            corfuMsg.serialize(buffer);
            list.add(buffer);
        } catch (Exception e) {
            log.error("Error during serialization!", e);
        }
    }
}
