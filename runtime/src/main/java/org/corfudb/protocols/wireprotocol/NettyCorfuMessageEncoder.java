package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;

import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.LongAccumulator;

/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
@Sharable
public class NettyCorfuMessageEncoder extends MessageToMessageEncoder<CorfuMsg> {


    private final LongAccumulator maxValue = new LongAccumulator(Math::max, Long.MIN_VALUE);

    /** {@inheritDoc} */
    @Override
    protected void encode(
        @Nonnull ChannelHandlerContext ctx,
        @Nonnull CorfuMsg corfuMsg,
        @Nonnull List<Object> list) throws Exception {
        // Nice if the size could be known
        final ByteBuf buffer = ctx.alloc().directBuffer();
        try {
            corfuMsg.serialize(buffer);
            if (log.isDebugEnabled()) {
                long prev = maxValue.get();
                maxValue.accumulate(buffer.readableBytes());
                long curr = maxValue.get();
                // The max value has been updated.
                if (prev < curr) {
                    log.debug("encode: New max write buffer found {}", curr);
                }
            }
            list.add(buffer);
        } catch (Exception e) {
            log.error("encode: Error during serialization!", e);
        }
    }
}
