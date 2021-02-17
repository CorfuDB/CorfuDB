package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.CorfuProtocolCommon.MessageMarker;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;

/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
public class NettyCorfuMessageDecoder extends ByteToMessageDecoder {

    /**
     * Decodes an inbound corfu message from a ByteBuf. The corfu message is either
     * legacy (of type CorfuMsg) or Protobuf (of type RequestMsg/ResponseMsg).
     *
     * @param channelHandlerContext   the Netty channel handler context
     * @param byteBuf                 the underlying ByteBuf
     * @param list                    a list of decoded objects given to the
     *                                next pipeline handler
     */
    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf,
                          List<Object> list) throws Exception {
        // Check the type of message based on first byte
        byte msgMark = byteBuf.readByte();

        switch (MessageMarker.typeMap.get(msgMark)) {
            case PROTO_REQUEST_MSG_MARK:
                try (ByteBufInputStream msgInputStream = new ByteBufInputStream(byteBuf)) {
                    try {
                        RequestMsg request = RequestMsg.parseFrom(msgInputStream);
                        list.add(request);
                    } catch (IOException e) {
                        log.error("decode: An exception occurred during parsing request "
                                + "from ByteBufInputStream.", e);
                    }
                }

                break;
            case PROTO_RESPONSE_MSG_MARK:
                try (ByteBufInputStream msgInputStream = new ByteBufInputStream(byteBuf)) {
                    try {
                        ResponseMsg response = ResponseMsg.parseFrom(msgInputStream);
                        list.add(response);
                    } catch (IOException e) {
                        log.error("decode: An exception occurred during parsing response "
                                + "from ByteBufInputStream.", e);
                    }
                }

                break;
            default:
                throw new IllegalStateException("decode: Received an incorrectly marked message.");
        }
    }

    @Override
    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in,
                              List<Object> out) throws Exception {
        //log.info("Netty channel handler context goes inactive, received out size is {}",
        // (out == null) ? null : out.size());

        if (in != Unpooled.EMPTY_BUFFER) {
            this.decode(ctx, in, out);
        }
        // ignore the Netty generated {@link EmptyByteBuf empty ByteBuf message} when channel
        // handler goes inactive (typically happened after each received burst of batch of messages)
    }
}
