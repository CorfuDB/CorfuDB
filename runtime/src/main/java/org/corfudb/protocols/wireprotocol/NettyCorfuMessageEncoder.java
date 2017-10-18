package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
public class NettyCorfuMessageEncoder extends MessageToByteEncoder<CorfuMsg> {


    Integer max = 0;

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext,
                          CorfuMsg corfuMsg,
                          ByteBuf byteBuf) throws Exception {
        try {
            corfuMsg.serialize(byteBuf);
            byteBuf.readableBytes();
            synchronized (max) {
                if (max < byteBuf.readableBytes()) {
                    max = byteBuf.readableBytes();
                    log.debug("New write buffer max found {}", max);
                }
            }

        } catch (Exception e) {
            log.error("Error during serialization!", e);
        }
    }
}
