package org.corfudb.common.protocol;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.LegacyCorfuMsg;
import org.corfudb.common.protocol.proto.CorfuProtocol.UniversalMsg;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class UniversalMsgEncoder extends MessageToByteEncoder<Object> {
    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext,
                          Object msg, ByteBuf out) {
        if(msg instanceof ByteBuf) {
            byte[] bytes = new byte[((ByteBuf)msg).readableBytes()];
            ((ByteBuf)msg).readBytes(bytes); // TODO: why not use .array()?

            CorfuProtocol.LegacyCorfuMsg lcm = CorfuProtocol.LegacyCorfuMsg.newBuilder().
                    setPayload(ByteString.copyFrom(bytes)).build();

            CorfuProtocol.UniversalMsg universalMsg = CorfuProtocol.UniversalMsg.
                    newBuilder().setLegacyCorfuMsg(lcm).build();

            out.writeBytes(universalMsg.toByteArray());
        }
        else if(msg instanceof CorfuProtocol.Request) {
            CorfuProtocol.UniversalMsg universalMsg = CorfuProtocol.UniversalMsg.
                    newBuilder().setMsgRequest((CorfuProtocol.Request)msg).build();
            log.info("universalMsg byte size is {}",universalMsg.getSerializedSize());
            out.writeBytes(universalMsg.toByteArray());
        }
        else if(msg instanceof CorfuProtocol.Response) {
            CorfuProtocol.UniversalMsg universalMsg = CorfuProtocol.UniversalMsg.
                    newBuilder().setMsgResponse((CorfuProtocol.Response)msg).build();
            log.info("universalMsg byte size is {}",universalMsg.getSerializedSize());
            out.writeBytes(universalMsg.toByteArray());
        }
    }
}