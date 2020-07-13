package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.API;


/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
public class NettyCorfuMessageDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf,
                          List<Object> list) throws Exception {
        // Leave message unchanged if is non-legacy
        if(byteBuf.getByte(byteBuf.readerIndex()) != API.LEGACY_CORFU_MSG_MARK) {
            list.add(byteBuf);
            return;
        }

        byteBuf.readByte();
        list.add(CorfuMsg.deserialize(byteBuf));
    }
}
