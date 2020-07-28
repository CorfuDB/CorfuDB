package org.corfudb.infrastructure.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.corfudb.common.protocol.API;
import org.corfudb.common.protocol.proto.CorfuProtocol.MessageType;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.runtime.view.Layout;

import java.io.IOException;
import java.util.*;

@Slf4j
@ChannelHandler.Sharable
public class NettyServerRouter extends ChannelInboundHandlerAdapter implements IServerRouter {

    /**
     * This map stores the mapping from message types to server handler.
     */
    private final Map<MessageType, AbstractServer> handlerMap;

    /**
     * This node's server context.
     */
    private final ServerContext serverContext;

    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    @Getter
    @Setter
    volatile long serverEpoch;

    /** The {@link AbstractServer}s this {@link NettyServerRouter} routes messages for. */
    private final ImmutableList<AbstractServer> servers;

    /**
     * Construct a new NettyServerRouter.
     * @param servers A list of {@link AbstractServer}s this router will route requests for.
     * @param serverContext The server context.
     */
    public NettyServerRouter(ImmutableList<AbstractServer> servers, ServerContext serverContext) {
        this.serverContext = serverContext;
        this.serverEpoch = serverContext.getServerEpoch();
        this.servers = servers;
        handlerMap = new EnumMap<>(MessageType.class);

        servers.forEach(server -> {
            Set<MessageType> handledTypes = server.getHandlerMethods().getHandledTypes();
            handledTypes.forEach(handledType -> handlerMap.put(handledType, server));
        });
    }

    @Override
    public Optional<Layout> getCurrentLayout() {
        return Optional.ofNullable(serverContext.getCurrentLayout());
    }

    /**
     * Send a response message through this router.
     * @param response The response message to send.
     * @param ctx The context of the channel handler.
     */
    @Override
    public void sendResponse(Response response, ChannelHandlerContext ctx) {
        ByteBuf outBuf = PooledByteBufAllocator.DEFAULT.buffer();
        ByteBufOutputStream responseOutputStream = new ByteBufOutputStream(outBuf);

        try {
            responseOutputStream.writeByte(API.PROTO_CORFU_MSG_MARK);
            response.writeTo(responseOutputStream);
            ctx.writeAndFlush(outBuf);
        } catch(IOException e) {
            log.warn("Exception occurred when sending response {}, caused by {}", response.getHeader(), e.getCause(), e);
        } finally {
            IOUtils.closeQuietly(responseOutputStream);
            outBuf.release();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated This operation is no longer supported. The router will only route messages for
     * servers provided at construction time.
     */
    @Override
    @Deprecated
    public void addServer(AbstractServer server) {
        throw new UnsupportedOperationException("No longer supported");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AbstractServer> getServers() {
        return servers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setServerContext(ServerContext serverContext) {
        throw new UnsupportedOperationException("The operation is not supported.");
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("channelActive: Incoming connection established from: {}.",
                ctx.channel().remoteAddress());
        ctx.fireChannelActive(); // So that handshake is initiated.
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf msgBuf = (ByteBuf) msg;

        // Temporary -- If message is a legacy message, forward the message.
        byte msgMark = msgBuf.getByte(msgBuf.readerIndex());
        if (msgMark == API.LEGACY_CORFU_MSG_MARK) {
            ctx.fireChannelRead(msgBuf);
            return;
        } else if(msgMark != API.PROTO_CORFU_MSG_MARK) {
            throw new IllegalStateException("Received incorrectly marked message.");
        }

        msgBuf.readByte();
        ByteBufInputStream msgInputStream = new ByteBufInputStream(msgBuf);

        try {
            Request request = Request.parseFrom(msgInputStream);
            Header header = request.getHeader();

            if (log.isDebugEnabled()) {
                log.debug("Request {} from {}", header.getType(), ctx.channel().remoteAddress());
            }

            AbstractServer handler = handlerMap.get(header.getType());
            if (handler == null) {
                log.warn("Received unregistered request message {}, dropping", header.getType());
            } else {
                if(requestIsValid(request, ctx)) {
                    if(log.isTraceEnabled()) {
                        log.trace("Request message routed to {}: {}", handler.getClass().getSimpleName(), request);
                    }

                    try {
                        handler.handleRequest(request, ctx, this);
                    } catch(Throwable t) {
                        log.error("channelRead: Handling {} failed due to {}:{}",
                                header.getType(),t.getClass().getSimpleName(), t.getMessage(), t);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception during read!", e);
        } finally {
            msgInputStream.close();
            msgBuf.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in handling inbound message, {}", cause);
        ctx.close();
    }
}
