package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;

import static org.corfudb.protocols.CorfuProtocolCommon.LEGACY_CORFU_MSG_MARK;
import static org.corfudb.protocols.CorfuProtocolCommon.PROTO_CORFU_REQUEST_MSG_MARK;
import static org.corfudb.protocols.CorfuProtocolCommon.PROTO_CORFU_RESPONSE_MSG_MARK;

/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
public class NettyCorfuMessageDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf,
                          List<Object> list) throws Exception {
        // Check the type of message based on first byte
        byte msgMark = byteBuf.readByte();

        if (msgMark == LEGACY_CORFU_MSG_MARK) {
            list.add(CorfuMsg.deserialize(byteBuf));
        } else if (msgMark == PROTO_CORFU_REQUEST_MSG_MARK){
            ByteBufInputStream msgInputStream = new ByteBufInputStream(byteBuf);
            try {
                RequestMsg request = RequestMsg.parseFrom(msgInputStream);
                list.add(request);
            } catch (Exception e) {
                log.error("decode: An exception occurred during parsing request from ByteBufInputStream of byteBuf.", e);
            } finally {
                msgInputStream.close();
            }
        } else if (msgMark == PROTO_CORFU_RESPONSE_MSG_MARK){
            ByteBufInputStream msgInputStream = new ByteBufInputStream(byteBuf);
            try {
                ResponseMsg response = ResponseMsg.parseFrom(msgInputStream);
                list.add(response);
            } catch (Exception e) {
                log.error("decode: An exception occurred during parsing response from ByteBufInputStream.", e);
            } finally {
                msgInputStream.close();
            }
        } else {
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
