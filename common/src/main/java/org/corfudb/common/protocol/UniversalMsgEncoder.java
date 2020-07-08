package org.corfudb.common.protocol;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.corfudb.common.protocol.proto.CorfuProtocol;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class UniversalMsgEncoder extends MessageToMessageEncoder<Object> {
    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext,
                          Object msg, List<Object> out) {
        if(msg instanceof ByteBuf) {
            byte[] bytes = new byte[((ByteBuf)msg).readableBytes()];
            ((ByteBuf)msg).readBytes(bytes); // TODO: why not use .array()?

            CorfuProtocol.LegacyCorfuMsg lcm = CorfuProtocol.LegacyCorfuMsg.newBuilder().
                    setPayload(ByteString.copyFrom(bytes)).build();

            CorfuProtocol.UniversalMsg universalMsg = CorfuProtocol.UniversalMsg.
                    newBuilder().setLegacyCorfuMsg(lcm).build();

            out.add(universalMsg);
        }
    }
}