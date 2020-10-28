package org.corfudb.runtime.clients;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.buffer.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.corfudb.protocols.CorfuProtocolCommon.LEGACY_CORFU_MSG_MARK;
import static org.corfudb.protocols.CorfuProtocolCommon.PROTO_CORFU_REQUEST_MSG_MARK;
import static org.corfudb.protocols.CorfuProtocolCommon.PROTO_CORFU_RESPONSE_MSG_MARK;

/**
 * Created by mwei on 7/6/16.
 */
@Slf4j
public class TestChannelContext implements ChannelHandlerContext {

    @FunctionalInterface
    interface HandleMessageFunction {
        void handleMessage(Object o);
    }

    HandleMessageFunction hmf;

    private final static int THREAD_COUNT = 4;
    private final static ExecutorService msgExecutorService = Executors.newFixedThreadPool(THREAD_COUNT,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("testChannel-%d").build());

    public TestChannelContext(HandleMessageFunction hmf) {
        this.hmf = hmf;
    }

    void sendMessageAsync(Object o, Object orig) {
        CompletableFuture.runAsync(() -> {
            try {
                Object origCapture = orig; //for debugging
                if (o instanceof ByteBuf) {
                    byte msgMark = ((ByteBuf) o).readByte();

                    if (msgMark == LEGACY_CORFU_MSG_MARK) {
                        CorfuMsg m = CorfuMsg.deserialize((ByteBuf) o);
                        hmf.handleMessage(m);
                        ((ByteBuf) o).release();
                    } else if (msgMark == PROTO_CORFU_REQUEST_MSG_MARK) {
                        try (ByteBufInputStream msgInputStream = new ByteBufInputStream((ByteBuf) o)) {
                            RequestMsg requestMsg = RequestMsg.parseFrom(msgInputStream);
                            hmf.handleMessage(requestMsg);
                            ((ByteBuf) o).release();
                        }
                    } else if (msgMark == PROTO_CORFU_RESPONSE_MSG_MARK) {
                        try (ByteBufInputStream msgInputStream = new ByteBufInputStream((ByteBuf) o)) {
                            ResponseMsg responseMsg = ResponseMsg.parseFrom(msgInputStream);
                            hmf.handleMessage(responseMsg);
                            ((ByteBuf) o).release();
                        }
                    }
                }
            } catch (Exception e) {
                 log.warn("Error during deserialization", e);
            }
        }, msgExecutorService);
    }

    @Override
    public Channel channel() {
        return null;
    }

    @Override
    public EventExecutor executor() {
        return null;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public ChannelHandler handler() {
        return null;
    }

    @Override
    public boolean isRemoved() {
        return false;
    }

    @Override
    public ChannelHandlerContext fireChannelRegistered() {
        return null;
    }

    @Override
    public ChannelHandlerContext fireChannelUnregistered() {
        return null;
    }

    @Override
    public ChannelHandlerContext fireChannelActive() {
        return null;
    }

    @Override
    public ChannelHandlerContext fireChannelInactive() {
        return null;
    }

    @Override
    public ChannelHandlerContext fireExceptionCaught(Throwable throwable) {
        return null;
    }

    @Override
    public ChannelHandlerContext fireUserEventTriggered(Object o) {
        return null;
    }

    @Override
    public ChannelHandlerContext fireChannelRead(Object o) {
        return null;
    }

    @Override
    public ChannelHandlerContext fireChannelReadComplete() {
        return null;
    }

    @Override
    public ChannelHandlerContext fireChannelWritabilityChanged() {
        return null;
    }

    @Override
    public ChannelFuture bind(SocketAddress socketAddress) {
        return null;
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress) {
        return null;
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress, SocketAddress socketAddress1) {
        return null;
    }

    @Override
    public ChannelFuture disconnect() {
        return null;
    }

    @Override
    public ChannelFuture close() {
        return null;
    }

    @Override
    public ChannelFuture deregister() {
        return null;
    }

    @Override
    public ChannelFuture bind(SocketAddress socketAddress, ChannelPromise channelPromise) {
        return null;
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress, ChannelPromise channelPromise) {
        return null;
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress, SocketAddress socketAddress1, ChannelPromise channelPromise) {
        return null;
    }

    @Override
    public ChannelFuture disconnect(ChannelPromise channelPromise) {
        return null;
    }

    @Override
    public ChannelFuture close(ChannelPromise channelPromise) {
        return null;
    }

    @Override
    public ChannelFuture deregister(ChannelPromise channelPromise) {
        return null;
    }

    @Override
    public ChannelHandlerContext read() {
        return null;
    }

    @Override
    public ChannelFuture write(Object o) {
        ByteBuf b = simulateSerialization(o);
        sendMessageAsync(b,o);
        return null;
    }

    @Override
    public ChannelFuture write(Object o, ChannelPromise channelPromise) {
        ByteBuf b = simulateSerialization(o);
        sendMessageAsync(b,o);
        return null;
    }

    @Override
    public ChannelHandlerContext flush() {
        return null;
    }

    @Override
    public ChannelFuture writeAndFlush(Object o, ChannelPromise channelPromise) {
        ByteBuf b = simulateSerialization(o);
        sendMessageAsync(b,o);
        return null;
    }

    @Override
    public ChannelFuture writeAndFlush(Object o) {
        ByteBuf b = simulateSerialization(o);
        sendMessageAsync(b,o);
        return null;
    }

    public ByteBuf simulateSerialization(Object message) {
        ByteBuf oBuf = Unpooled.buffer();

        if (message instanceof CorfuMsg) {
            CorfuMsg corfuMsg = (CorfuMsg) message;
            // Marks the Corfu msg as legacy.
            oBuf.writeByte(LEGACY_CORFU_MSG_MARK);
            corfuMsg.serialize(oBuf);
            oBuf.resetReaderIndex();
        } else if (message instanceof RequestMsg) {
            RequestMsg requestMsg = (RequestMsg) message;
            try (ByteBufOutputStream requestOutputStream = new ByteBufOutputStream(oBuf)) {
                // Marks the Corfu msg as a protobuf request.
                requestOutputStream.writeByte(PROTO_CORFU_REQUEST_MSG_MARK);
                requestMsg.writeTo(requestOutputStream);
            } catch (IOException e) {
                log.error("simulateSerialization: Exception occurred when encoding request {}, caused by {}",
                        requestMsg, e.getCause(), e);
            }
        } else if (message instanceof ResponseMsg) {
            ResponseMsg responseMsg = (ResponseMsg) message;
            try (ByteBufOutputStream requestOutputStream = new ByteBufOutputStream(oBuf)) {
                // Marks the Corfu msg as a protobuf response.
                requestOutputStream.writeByte(PROTO_CORFU_RESPONSE_MSG_MARK);
                responseMsg.writeTo(requestOutputStream);
            } catch (IOException e) {
                log.error("simulateSerialization: Exception occurred when encoding response {}, caused by {}",
                        responseMsg, e.getCause(), e);
            }
        } else {
            throw new UnsupportedOperationException("Test framework does not support serialization of object type "
                    + message.getClass());
        }

        return oBuf;
    }

    @Override
    public ChannelPipeline pipeline() {
        return null;
    }

    @Override
    public ByteBufAllocator alloc() {
        return null;
    }

    @Override
    public ChannelPromise newPromise() {
        return null;
    }

    @Override
    public ChannelProgressivePromise newProgressivePromise() {
        return null;
    }

    @Override
    public ChannelFuture newSucceededFuture() {
        return null;
    }

    @Override
    public ChannelFuture newFailedFuture(Throwable throwable) {
        return null;
    }

    @Override
    public ChannelPromise voidPromise() {
        return null;
    }

    @Override
    public <T> Attribute<T> attr(AttributeKey<T> attributeKey) {
        return null;
    }

    /**
     * @param attributeKey
     * @deprecated
     */
    @Override
    public <T> boolean hasAttr(AttributeKey<T> attributeKey) {
        return false;
    }
}
