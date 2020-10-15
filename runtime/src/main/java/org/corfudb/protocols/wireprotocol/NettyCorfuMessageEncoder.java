package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;
import java.util.concurrent.atomic.LongAccumulator;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.corfudb.protocols.API;
import org.corfudb.protocols.wireprotocol.orchestrator.Request;
import org.corfudb.runtime.protocol.proto.CorfuProtocol;

/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
public class NettyCorfuMessageEncoder extends MessageToByteEncoder<Object> {


    final LongAccumulator maxValue = new LongAccumulator(Math::max, Long.MIN_VALUE);

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext,
                          Object object,
                          ByteBuf byteBuf) throws Exception {
        try {
            if (object instanceof CorfuMsg) {
                CorfuMsg corfuMsg = (CorfuMsg) object;
                byteBuf.writeByte(API.LEGACY_CORFU_MSG_MARK); // Temporary -- Marks the Corfu msg as legacy.
                corfuMsg.serialize(byteBuf);
            } else if (object instanceof CorfuProtocol.Request) {
                CorfuProtocol.Request request = (CorfuProtocol.Request) object;
                ByteBufOutputStream requestOutputStream = new ByteBufOutputStream(byteBuf);
                try {
                    requestOutputStream.writeByte(API.PROTO_CORFU_REQUEST_MSG_MARK); // Temporary - Marks the Corfu msg as protobuf.
                    request.writeTo(requestOutputStream);
                } catch(IOException e) {
                    log.warn("encode[{}]: Exception occurred when encoding request {}, caused by {}",
                            request.getHeader().getRequestId(), request.getHeader(), e.getCause(), e);
                } finally {
                    IOUtils.closeQuietly(requestOutputStream);
                }
            } else if (object instanceof CorfuProtocol.Response) {
                CorfuProtocol.Response response = (CorfuProtocol.Response) object;
                ByteBufOutputStream responseOutputStream = new ByteBufOutputStream(byteBuf);
                try {
                    responseOutputStream.writeByte(API.PROTO_CORFU_RESPONSE_MSG_MARK); // Temporary - Marks the Corfu msg as protobuf.
                    response.writeTo(responseOutputStream);
                } catch(IOException e) {
                    log.warn("encode[{}]: Exception occurred when encoding response {}, caused by {}",
                            response.getHeader().getRequestId(), response.getHeader(), e.getCause(), e);
                } finally {
                    IOUtils.closeQuietly(responseOutputStream);
                }
            } else {
                log.error("encode: Unknown object of class - {} received while encoding", object.getClass());
            }

            if (log.isDebugEnabled()) {
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
