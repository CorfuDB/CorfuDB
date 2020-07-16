package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.LongAccumulator;

/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
public class NettyCorfuMessageEncoder extends MessageToByteEncoder<CorfuMsg> {


    final LongAccumulator maxValue = new LongAccumulator(Math::max, Long.MIN_VALUE);

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext,
                          CorfuMsg corfuMsg,
                          ByteBuf byteBuf) throws Exception {
        try {
            byteBuf.writeByte(0x1); // Temporary -- Marks the Corfu msg as legacy
            corfuMsg.serialize(byteBuf);
            if(log.isDebugEnabled()) {
                long prev = maxValue.get();
                maxValue.accumulate(byteBuf.readableBytes());
                long curr = maxValue.get();
                // The max value has been updated.
                if (prev < curr) {
                    log.debug("encode: New max write buffer found {}", curr);
                }
            }

        } catch (Exception e) {
            log.error("encode: Error during serialization!", e);
        }
    }
}
