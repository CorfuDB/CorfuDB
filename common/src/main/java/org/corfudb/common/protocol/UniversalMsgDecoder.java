package org.corfudb.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.corfudb.common.protocol.proto.CorfuProtocol;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class UniversalMsgDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext,
                          ByteBuf in, List<Object> out) throws Exception {

        try (ByteBufInputStream msgInputStream = new ByteBufInputStream(in)) {
            CorfuProtocol.UniversalMsg universalMsg = CorfuProtocol.UniversalMsg.parseFrom(msgInputStream);
            if (universalMsg.hasLegacyCorfuMsg()) {
                // Add to the output pipeline to be handled next by NettyCorfuMessageDecoder
                ByteBuf buf = Unpooled.copiedBuffer(universalMsg.getLegacyCorfuMsg().getPayload().toByteArray());
                out.add(buf);
            } else if (universalMsg.hasMsgRequest()) {
                // TODO handle the Protobuf Message Requests
                log.info(universalMsg.getMsgRequest().toString());
            } else if (universalMsg.hasMsgResponse()) {
                // TODO handle the Protobuf Message Responses
                log.info(universalMsg.getMsgResponse().toString());
            } else {
                log.error("UniversalMsgDecoder: Unknown/Unregistered UniversalMsg was received and discarded.");
            }
        } finally {
            in.release();
        }
    }
}