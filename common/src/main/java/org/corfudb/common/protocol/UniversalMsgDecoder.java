package org.corfudb.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.corfudb.common.protocol.proto.CorfuProtocol;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class UniversalMsgDecoder extends MessageToMessageDecoder<CorfuProtocol.UniversalMsg> {
    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext,
                          CorfuProtocol.UniversalMsg universalMsg,
                          List<Object> out) throws Exception {
        if(universalMsg.hasLegacyCorfuMsg()) {
            ByteBuf buf = Unpooled.copiedBuffer(universalMsg.getLegacyCorfuMsg().getPayload().toByteArray());
            out.add(buf);
        }

    }
}